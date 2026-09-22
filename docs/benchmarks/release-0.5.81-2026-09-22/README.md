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
