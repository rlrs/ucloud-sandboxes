# Gateway routing write batching

This extends the prior [worker maintenance improvements](overload-maintenance-2026-09-22.md).
No production endpoints, services, credentials or workloads were accessed. Tests
and benchmarks ran in the isolated Linux checkout on `rasmus-dev`.

## Problem and changes

The saved [gateway profile](../benchmarks/relay-load-2026-09-22/gateway-profile23.json)
recorded 4,375 program-transition transactions with 76.2 seconds of aggregate
writer wait, versus 2.5 seconds holding the transaction body. Routing remains
SQLite after the relay authority's PostgreSQL cutover.

RoutingStore now uses the existing DurableSqliteBatch mechanism. Store instances
for the same path, database inode and process share a writer. Concurrent operations
use separate savepoints, retaining generation, ownership and capacity checks, and
share a FULL WAL commit. The default grouping window is 1 ms, with up to 64
operations per transaction. These parameters govern batching, not admission.
Readers keep separate connections and see committed state. Callers wait until
commit succeeds; commit failure fails all waiting operations. Failed savepoints
cannot overwrite another operation. Replaced databases and inherited post-fork
writers are rejected. Cross-process exclusion remains SQLite's responsibility.

Program-state transitions also select only the current sandbox generation for
validation. Previously they loaded and decoded the entire route, including its
specification and storage snapshot, while holding the writer. The reduced query
still rejects missing sandboxes and stale generations inside the same transaction.

No schema change, production migration, SDK change or reduced durability policy
is required. A lone routing write can pay the extra 1 ms batching window; the
optimization targets concurrent traffic.

## Linux measurements

The [benchmark](../benchmarks/routing-batching-2026-09-22/linux-benchmark.json)
compares the prior one-commit-per-operation implementation against batching using
512 actual RoutingStore exec-route writes and 32 submitting threads:

| Filesystem case | Original | Batched |
| --- | ---: | ---: |
| Total elapsed, no injected delay | 1.679 s | 0.311 s |
| Write p95, no injected delay | 110.8 ms | 36.9 ms |
| Commits, no injected delay | 512 | 54 |
| Total elapsed, simulated 5 ms commit delay | 4.366 s | 0.654 s |
| Write p95, simulated 5 ms commit delay | 280.5 ms | 78.8 ms |

An additional query microbenchmark uses a synthetic 11.9 KB sandbox specification.
10,000 generation validations take 304 ms via full-route decoding and 27 ms via
the generation-only projection. This isolates query/decoding cost; it is not a
measurement of the complete program-state transition.

The benchmark also SIGKILLs a separate routing writer before commit and after its
success acknowledgment. Recovery finds zero rows before commit and the acknowledged
row after it. Both databases pass integrity_check. This tests process-crash
recovery, not physical power loss.

A directory-fsync batching prototype for hibernation JSON journals was rejected.
It reduced flush count but worsened elapsed time and p95 in both tested cases.
The [trial results](../benchmarks/routing-batching-2026-09-22/rejected-directory-batching-trial.json)
are retained for comparison; that implementation is not included. Hibernation
journal format, fsync ordering and synchronous checkpoint cleanup are unchanged.

## Validation

The full Linux suite ran **1,186 tests**, with **61 environment-dependent skips**,
and passed. Ruff and the diff whitespace check also passed.

The new regressions cover shared writers across separate stores, committed-only
reads during a pending batch, conflict isolation, durability acknowledgment,
stale/missing sandbox generation checks, database replacement, and fork rejection.
The existing batch suite covers failed commits and SQLite-level transaction aborts.
Exact tested [source hashes](../benchmarks/routing-batching-2026-09-22/sources.json)
and the final Linux suite output are retained beside the benchmark.

Reproduce without production access:

```sh
PYTHONPATH=. python scripts/benchmark_lifecycle_batch.py
PYTHONPATH=. python -m unittest discover -s tests
```

These are isolated component benchmarks. The loaded native wake p95 remains to be
measured when production qualification is authorized; these results do not establish
the 0.8-second end-to-end target.
