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
