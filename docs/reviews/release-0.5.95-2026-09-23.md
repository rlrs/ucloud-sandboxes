# Isolate fleet response rendering from lifecycle commits

The .94 production profile placed about 43% of captured Python execution in
fleet-list rendering. Those CPU-heavy JSON/dataclass operations share the GIL
with short SQLite transactions, which accumulated a queue even though worker
wake responses were normally around 100 ms or less.

The production gateway now renders bulk list snapshots in one spawned process.
An anonymous pipe carries fixed read commands and JSON bytes; there is no new
network endpoint. Each request still reads the current routing and heartbeat
state, including external writes and freshness checks. Concurrent HTTP polls
retain existing coalescing. No TTL, stale-success fallback, lifecycle admission
limit, relaxed durability, or extra gateway CPU allocation is introduced.

A failed child is reaped and restarted for one safe read retry. Invalid state
fails closed. Server close shuts down the reader. Tests cover fresh external
updates/deletes, corrupted state, killed-child recovery, HTTP integration and
cleanup. All 96 Linux gateway/relay/reader tests passed.

A four-CPU-affinity Linux component comparison with concurrent fleet reads
and durable routing writes improved 668 to 995 requests/s and p95 215 to 36 ms.
This is a component result, not the full-load SLO. The attempted separate-writer
process provided no useful tail improvement and was not implemented.

On the actual four-vCPU gateway/Python 3.14, the same comparison improved p95
from 615 to 81 ms and median from 69 to 52 ms. Total throughput decreased
242 to 226 requests/s and maximum rose to 3.51 s, including lazy child startup.
This is evidence of improved latency isolation, not a throughput win or final
qualification. The full workload must still measure creation, relay work and
worker behavior together.

Unprofiled production run `relay-load-42ec8783467e` (00:07–00:12 UTC):
2,048/2,048 cycles correct. Measured wake p95 1.286 s, response-to-first-usable
p95 1.900 s, median 0.662 s. Health p95 102 ms, fleet-list p95 171 ms;
no health, list, workload, or cleanup errors. Guest tool p95 was 63 ms.
Five existing idle workers were upgraded before this run; provider cold boot
was not exercised. All 256 guest sandboxes were newly created. There were
184 cycles overlapping provisioning (including warmup); their p95 was 1.856 s.
No measured natural cycle was observed parked. This is not an SLO pass.

The initial forced-park run failed before completing a cycle because the
harness used an SDK credential for a control-only park endpoint. Production
correctly returned HTTP 403. The harness now requires a separate gateway token
for forced mode; natural traffic continues to use the least-privileged SDK key.
