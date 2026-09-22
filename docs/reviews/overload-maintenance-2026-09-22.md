# Reclaim-aware parking, maintenance pacing and durable journal batching

Implemented following the [production investigation](storage-contention-2026-09-22.md).
Production was neither accessed nor modified for this work. Validation uses an
isolated checkout and Linux containers on `rasmus-dev`; no gateway endpoints,
production credentials, synthetic production sandboxes or service restarts.

## Avoid unnecessary short-wait checkpoints

Relay-triggered parks now wait briefly before acquiring the sandbox lifecycle
lock. The budget uses available host memory, memory pressure and observed response
latencies, incoming restore demand and the retained sandbox memory size. With ample memory it starts at at most fifteen seconds; lower headroom or
memory pressure shortens it, and pressure is rechecked every 50 ms. Missing memory
evidence gives zero grace. The policy samples Linux pressure at most every 100 ms
per runtime. Direct user-requested parks keep their immediate semantics.

A matching wake signals the waiting park before acquiring the lifecycle lock.
The park then yields to the wake. The existing durable request/generation wake
fence is checked both before the delay and under the lifecycle lock, so process
restart or loss of the in-memory notification cannot authorize a late park.
A generation or request mismatch cannot cancel another park. Replays share the
original start time rather than continually extending the grace period.

Prediction history includes responses arriving after a park, avoiding a bias
where sampling only cancelled parks would continuously shorten the budget. It is
bounded disposable metadata; evicting history does not delete lifecycle authority
or impose an admission limit. The response estimate uses the 90th percentile of
recent observed waits. Incoming restore demand includes requests still waiting
for a slot and is deduplicated against active reservations. Queued cold starts or
a draining node end warm retention. Rechecking every 50 ms lets it yield to new
demand without waiting for the original timer. Savings under production load
remain to be measured.

## Pace compaction behind foreground work

Local checkpoint compaction now applies backpressure between output chunks.
It smoothly reduces its duty cycle with I/O PSI; active foreground storage work
also gets preference before the pressure average catches up. The policy allows
continued maintenance progress, bounds each cooperative pause to 100 ms, and
removes the delay when both foreground work and pressure subside. It introduces
no fixed MiB/s ceiling or client-facing admission error.

The native control connection now supports progress-aware export waiting. Bytes
received on the export stream refresh progress, including partial chunks, and
intentional pacing is accounted for. A genuinely stalled export still times out.
This avoids timing out the separate control response merely because an export
has deliberately slowed down. Other native commands retain their existing timeout
behavior. Source pinning, stream digest verification, owner checks, durable output
publication and journal-before-delete adoption ordering remain intact.

`local_compaction_paced_ms` reports accumulated pacing time. The preceding
per-volume compaction lock fix remains included.

## Share a durable commit across independent journal operations

Worker storage-journal writers now share a short SQLite transaction window
(default 1 ms, at most 64 operations per batch). These are batching parameters,
not limits on queued requests. Closed batches stop accepting new writers, so
sustained arrival cannot postpone the commit indefinitely.

Operations execute serially under separate savepoints. A rejected operation rolls
back its own savepoint; other operations can still commit. An SQLite-level abort
or commit failure fails all affected waiters. Callers receive success only after
the shared `synchronous=FULL` WAL commit and identity validation. Concurrent readers
use separate connections and see committed state only. Per-volume ownership,
revision checks, capacity reservations, replay identity, and SQLite's exclusion
of writers in other processes remain authoritative.

No schema migration or reduced fsync policy is involved. Batch statistics expose
operation/commit counts, failed batches, and separate accumulated queue wait,
transaction and commit milliseconds. Reading these statistics does not wait on a
slow database commit.

## Validation and practical limits

The final [Linux suite](../benchmarks/overload-maintenance-2026-09-22/linux-tests.txt)
ran 1,183 tests successfully, with 61 environment-dependent skips. The exact
[source hashes](../benchmarks/overload-maintenance-2026-09-22/sources.json) identify
the tested working tree, which includes earlier uncommitted work. Ruff and diff
whitespace checks passed for the changed files.

Linux regressions cover cancellation, generation isolation, memory-pressure
arrival, durable fencing after a new runtime is constructed, unbiased prediction,
cooperative pacing, live export progress versus stalled exports, savepoint
isolation, batch visibility, failed commits, whole-transaction aborts, and delayed
acknowledgment until commit.

The [isolated journal benchmark](../benchmarks/overload-maintenance-2026-09-22/journal-batching.json)
uses 512 writes and 32 submitting threads. On the test container's filesystem,
the original one-commit-per-write pattern took 1.592 seconds and the batched pattern
0.090 seconds, with 18 commits instead of 512. With an explicitly simulated 5 ms
commit delay, totals were 4.233 and 0.255 seconds (22 batched commits). These are journal microbenchmarks,
not native sandbox wake measurements or production speedup claims.

The same script killed a separate writer process before commit and after its
success acknowledgment. Recovery retained zero rows in the first case and the
acknowledged row in the second; both databases passed `integrity_check`.

Reproduce in an isolated Linux environment with the server dependencies installed:

```sh
PYTHONPATH=. python -m unittest tests.test_overload_maintenance tests.test_node_runtime
PYTHONPATH=. python scripts/benchmark_durable_batch.py
python -m unittest discover -s tests
```

Still required before claiming an end-to-end improvement: native sandbox load in
an isolated deployment, or production qualification when explicitly reauthorized.
Measure park/checkpoint count, dirty bytes, compaction throughput/backlog, journal
commit latency, and verified tool-execution wake p95 together. These changes do
not establish the requested 0.8-second loaded-wake SLO.
