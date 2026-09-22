# FIFO SQLite writer admission, 2026-09-22

On the four-vCPU gateway, slow wake traces spent seconds waiting inside the
gateway while workers sometimes replied in tens of milliseconds. The routing
writer queue accumulated far more wait time than transaction or commit time.

Previously every waiting writer raced for the next SQLite batch. Release
0.5.81 queues writers in arrival order and wakes only the next writer. Each
writer releases its turn before awaiting commit, preserving grouped durable
commits. Batch size, full durability, per-operation rollback, and database
ownership validation are unchanged. This does not impose a request limit.

## Qualification before deployment

The microbenchmark uses 128 concurrent threads, 512 requests, and four program
state transitions per request. Every case uses a fresh local SQLite database.
Order is before/after/after/before. It runs in an isolated process and does not
change the running gateway. `fifo-microbench.py` takes the baseline
`durable_batch.py` from commit 442230c as its argument; the candidate package
must be on PYTHONPATH.

| Four-vCPU production host, isolated process | Before | FIFO |
|---|---:|---:|
| Request p95, two runs | 640 / 511 ms | 285 / 273 ms |
| Process CPU, two runs | 2.058 / 1.823 s | 1.447 / 1.385 s |
| Wall time, two runs | 1.622 / 1.427 s | 1.101 / 1.050 s |

A separate Linux container also reduced p95 (281/266 ms to 211/210 ms), with a
smaller CPU reduction. These results demonstrate a useful local improvement;
they do not establish an end-to-end wake gain.

The full Linux suite passed 1,210 tests with ten environment-dependent skips.
The added test blocks a durable commit, queues eight writers in known order,
and verifies their committed row order across subsequent commits. Existing
tests cover shared batches, failed-operation rollback and ownership fences.

## Production deployment and natural workload

Deployed commit `4d9b6e8` to the four-vCPU gateway job 12399353 and all five
workers. Verified all 110 installed package files against the wheel, both boot
bundles against the worker kernel, public health and PostgreSQL relay authority.
The autoscaler resumed after the worker rollout. No client SDK change is needed.

The 256-agent, eight-cycle natural workload completed 2,048/2,048 cycles, with
zero workload, cleanup or health-probe errors. Excluding the first cycles:

| Metric | Four-vCPU 0.5.80 | Four-vCPU 0.5.81 |
|---|---:|---:|
| Wake p50 | 1.840 s | 1.077 s |
| Wake p95 | 5.624 s | 1.807 s |
| Wake p99 | 8.792 s | 2.180 s |
| Usable exec p95 | 7.169 s | 2.463 s |

This removes much of the four-vCPU regression, but wake p95 remains above the
earlier two-vCPU 0.5.80 result of 1.648 s and the 0.8 s target. The comparison
does not establish that the additional gateway CPUs improve latency.

Twelve steady-load samples averaged 2.412 busy cores out of four. Gateway CPU
averaged 95.5% of one core, relay 79.8%, PostgreSQL 36.9%, autoscaler 5.1%.
The gateway has little capacity within its current process despite spare host
CPU. This supports investigating gateway serialization and thread scheduling;
it does not by itself identify the GIL as the sole remaining cause.

## Forced parking and final health

The forced-park run completed 768/768 cycles, with zero workload, cleanup or
health-probe errors. Its 512 measured cycles had wake p50 2.846 s, p95 6.476 s,
p99 7.246 s and usable exec p95 8.546 s. Four-vCPU 0.5.80 had wake p95 9.195 s;
two-vCPU 0.5.80 had 6.597 s. The software fix helps the four-vCPU regression,
but these results still show no substantial gain from gateway capacity alone.

The retained forced-run traces include startup as well as measured cycles.
One 17.423 s gateway request spent 12.800 s inside the worker HTTP handler,
while its nested sandbox wake took 3.215 s and storage EnsureMounted 136 ms.
The gateway admission wait was another 2.224 s. These traces expose remaining
coordination/worker delays; they do not support blaming all latency on storage
throughput or gateway CPU. No further runtime tuning was applied during tests.

After cleanup, routing and prepared-capacity tables were empty, relay inflight
and delivery-pending counts were zero, and PostgreSQL remained authoritative.
Both public health endpoints reported 0.5.81. Gateway, relay, registry,
autoscaler, PostgreSQL and telemetry collector services were active.
The gateway remains at four vCPUs. The 0.8 s target remains unmet.
