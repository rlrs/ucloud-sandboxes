# Overload scheduling and local queues

The [48-build overload phase](overload/summary.json) completed all 48 distinct
build IDs in 172.190 seconds. It reached the intended bounds: 32 admitted
nonterminal builds and 16 executing builds across four builders, with maxima of
eight admitted and four executing on each builder. Admission limits held, but
the local queues prolonged the tail after other builders had free slots.

Public client latency was 77.345 seconds p50 / 162.455 seconds p95. Submission
was 11.327 / 55.693 seconds; the measured builder queue was 9.324 / 108.377
seconds. These are different distributions and **must not be added**. All 133
HTTP 503 observations occurred on submission requests, followed by successful
admission; polling recorded no HTTP failures. The harness did not retain error
codes, so these observations do not establish that every 503 was `builder_busy`.

## Build intervals behind the long queue

Every row below belongs to builder job `167927368`. Times are seconds after
**2026-09-29 08:12:09 UTC**, rounded to milliseconds; exact timestamps remain in
the source JSON. `Created` is worker admission, `Queued` is context-ready,
`Execution` is worker-thread start, and `Finished` is terminal publication.
Execution includes build/push and EROFS publication. Subsequent cleanup occupies
the thread briefly after `Finished` and is not included in interval overlap.

| Case | Build ID | Recipe | Created | Queued | Execution | Finished | Reported queue seconds |
|---|---|---|---:|---:|---:|---:|---:|
| 009 | 210bbba0-ad4e-488f-9a0f-4e7525d7ce3c | python-agent | 8.240 | 8.805 | 8.808 | 119.897 | 0.003 |
| 018 | 8395ce00-6a12-4552-a66f-419903a25093 | python-agent | 8.268 | 8.816 | 8.818 | 119.895 | 0.002 |
| 026 | 8c9548a8-18e4-4423-94d0-e60cdd16e154 | typescript-multistage | 10.406 | 10.960 | 119.940 | 162.241 | 108.980 |
| 027 | 28337329-4815-472d-9d50-ba5ee0a0922b | python-agent | 8.211 | 8.729 | 8.732 | 119.897 | 0.003 |
| 033 | 5e75979a-6923-4954-a45a-548ffa24b5b0 | python-agent | 9.059 | 9.181 | 9.184 | 119.830 | 0.002 |
| 040 | a4dd569d-37ea-4927-b1fc-801e0af51edf | typescript-tools | 11.161 | 11.603 | 119.967 | 171.828 | 108.363 |
| 044 | 4cd0f3d1-ebe9-446b-9d46-ce8beb4d3057 | typescript-multistage | 10.400 | 10.955 | 119.855 | 162.313 | 108.899 |
| 047 | 5fbf1d05-e83b-4f17-8fa7-9e135f37583b | typescript-multistage | 11.129 | 11.568 | 119.953 | 162.218 | 108.385 |

Four Python builds occupied this builder's execution slots for approximately
111 seconds each. Four TypeScript builds were already admitted and context-ready
at seconds 11–12, but waited about 109 seconds. This establishes waiting behind
the four executing builds; the interval table alone does not identify why those
Python builds were slow.

## Fleet capacity while those requests waited

Cells show **executing / queued** from the exact build timestamps. The configured
execution limit was four per builder, or sixteen fleet-wide. A free execution
slot is not a promise that another arbitrary build would fit its CPU, memory,
disk or cache constraints without further checks.

| Offset seconds | Builder 167927353 | Builder 167927354 | Builder 167927367 | Builder 167927368 | Fleet executing / queued | Slots outside executing intervals |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 3 / 0 | 2 / 0 | 4 / 1 | 4 / 4 | 13 / 5 | 3 |
| 110 | 1 / 0 | 2 / 0 | 4 / 1 | 4 / 4 | 11 / 5 | 5 |
| 119 | 1 / 0 | 1 / 0 | 0 / 0 | 4 / 4 | 6 / 4 | 10 |
| 120 | 1 / 0 | 1 / 0 | 0 / 0 | 4 / 0 | 6 / 0 | 10 |
| 140 | 0 / 0 | 0 / 0 | 0 / 0 | 4 / 0 | 4 / 0 | 12 |

The longest continuous interval with queued work behind a full owner **and**
unused execution slots elsewhere was **23.880 seconds**, from offset
95.949897 to 119.829551 (08:13:44.949897–08:14:08.829551 UTC). Shorter instances
also exist, including ordinary subsecond cleanup/dispatch transitions; those
should not be presented as demonstrated avoidable delay.

The [implementation](../../../ucloud_sandboxes/images.py) keeps accepted work in
each `ImageManager._queued_builds` deque and starts it when that owner's active
thread count falls below four. The [gateway scheduler](../../../ucloud_sandboxes/control_plane.py)
places a new submission using current load, but does not move an already accepted
queued build to a newly available peer. These source contracts explain the
observed local waiting. The [full report](build-load-report.md) separately shows
builder CPU pressure and registry I/O; this scheduling evidence does not exclude
those costs.

**Follow-up candidate:** compare a shorter per-node queue, leaving more work
pending at the gateway until a slot is available, against the existing four-slot
queue budget. A central pull queue is a larger alternative. Either must preserve
build identity/single-flight behavior, context ownership, retry semantics and
drain safety. Context transfer, cache affinity and resource admission may offset
some benefit. No counterfactual time saving or increased fleet capacity is
claimed, and no scheduling settings were changed during this measurement.

## Reproduce from the retained JSON

Run this standard-library snippet from the repository root. It prints the eight
builder intervals, the snapshots above and the longest continuous waiting
interval. All intervals are half-open: finishing work releases its measured
execution interval before another start at the same timestamp. The four-slot
limit is the configuration used for this qualification.

```python
from collections import defaultdict
from datetime import datetime
from itertools import pairwise
from pathlib import Path
import json

path = Path("docs/benchmarks/build-load-2026-09-29/overload/summary.json")
records = json.loads(path.read_text())["records"]
def ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()

base = min(ts(r["started_at"]) for r in records)
rows = []
for r in records:
    b = r["build"]
    rows.append(dict(index=r["index"], build_id=b["build_id"],
                     owner=b["node"]["job_id"],
                     created=ts(b["created_at"]) - base,
                     queued=ts(b["queued_at"]) - base,
                     execution=ts(b["execution_started_at"]) - base,
                     finished=ts(b["finished_at"]) - base))
assert len({r["build_id"] for r in rows}) == 48
owners = sorted({r["owner"] for r in rows})

def counts(t):
    result = {owner: [0, 0] for owner in owners}  # executing, queued
    for r in rows:
        result[r["owner"]][0] += r["execution"] <= t < r["finished"]
        result[r["owner"]][1] += r["queued"] <= t < r["execution"]
    return result

for r in rows:
    if r["owner"] == "167927368":
        print(r)
for t in (100, 110, 119, 120, 140):
    print(t, counts(t))

boundaries = sorted({r[k] for r in rows for k in
                     ("queued", "execution", "finished")})
spans = []
for start, end in pairwise(boundaries):
    state = counts((start + end) / 2)
    full_owner_has_queue = any(active == 4 and queued for active, queued in state.values())
    free_elsewhere = sum(active for active, _ in state.values()) < 4 * len(owners)
    if full_owner_has_queue and free_elsewhere:
        if spans and spans[-1][1] == start:
            spans[-1][1] = end
        else:
            spans.append([start, end])
longest = max(spans, key=lambda s: s[1] - s[0])
print("longest", longest, "seconds", longest[1] - longest[0])
```
