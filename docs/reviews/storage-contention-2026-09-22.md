# Loaded production wake investigation — 2026-09-22

Investigated the real user workload following the PostgreSQL relay cutover.
The first completed change is a per-volume compaction lock fix. It is implemented
and Linux-tested, but not yet deployed. It is not a solution to the whole wake
latency problem.

## Evidence

The [initial monitoring](../benchmarks/production-monitor-2026-09-22/README.md)
measured 740 completed wakes with response-commit-to-delivery-release p50 5.82 s
and p95 16.06 s. Fleet size was growing; these are not steady-state or matched
before/after measurements. PostgreSQL had no sustained lock or pool backlog.

A subsequent 50-second profile temporarily wrapped storage methods and restored
all original methods afterward. On the workers:

| Job | Storage wake mount p50 | p95 | Journal update p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| 12398499 | 0.954 s | 2.142 s | 86 ms | 253 ms |
| 12398500 | 1.814 s | 4.397 s | 124 ms | 300 ms |
| 12398536 | 2.041 s | 3.710 s | 208 ms | 628 ms |
| 12398540 | 0.022 s | 0.083 s | 1 ms | 21 ms |

The last worker had only three measured mounts; it is a lightly loaded comparison,
not a statistically matched control. Measurements include operation wait time;
they do not isolate physical fsync from journal-lock contention. CPU time inside
these Python storage methods was small compared with elapsed time.

Two busy workers reported local compaction inputs of 97.9 GB and 86.7 GB, outputs
of 26.3 GB and 24.8 GB, with 31 and 36 volumes queued. Both had a compaction active.
These are cumulative counters since service startup, not rates. Neither had
completed a remote snapshot publication; registry publication is not established
as the source of their current storage writes. Compaction contributes work but
these counters do not establish its fraction of total disk traffic.

An eight-second `/proc/PID/io` sample on worker 12398499 attributed about
1,227 MiB/s of write_bytes to the native storage backend. Several live sandbox
runtime processes dirtied tens of MiB/s each with memory backing files open.
These counters include dirty-page accounting and child I/O after reaping; do not
sum them or equate them with actual block-device throughput. Separate diskstats
samples showed worker writes around 740–1,054 MiB/s and roughly 70% I/O PSI.
Further block/file-level attribution is needed before changing the native format.

## Implemented: isolate compaction manifest/adoption locks by volume

The compactor's node-wide lock covered journal persistence, file deletion,
manifest access and directory fsync. Adoption used a nonblocking lock attempt;
when another volume held it, unrelated wakes skipped already-prepared compacted
layers. Metrics used a blocking acquisition and could wait behind disk operations.

Those operations now hold per-volume locks. The shared lock covers only queue,
lock-map and counter operations. Same-volume adoption remains nonblocking and
serialized with manifest publication. Source ownership and journal-before-delete
ordering are unchanged. Weak lock-map entries disappear after the last holder or
waiter releases its reference, avoiding accumulation across deleted volumes.

A regression stalls one volume's journal commit, then checks that metrics and
adoption for a second volume complete before the stalled commit is released.
It also checks that same-volume adoption cannot overlap. The old implementation
fails the regression; the new implementation passes. All 67 selected Linux
storage/compaction/publication/journal tests passed. This demonstrates removal of
the coordination problem, not a measured production latency improvement.

## Larger fixes to qualify next

1. **Reduce parking work for short model waits when memory allows.** Existing
   external-backing checkpoints already avoid copying the entire application
   memory image. The larger opportunity is avoiding the whole checkpoint,
   filesystem seal, unmount and remount cycle for short waits. Defer parking
   according to available memory and observed response times, cancel promptly
   when the response arrives, and park immediately when reclaim is needed.
   Keep the existing request/epoch fence; a delayed park must never undo a wake.
   Measure checkpoint count and bytes per completed model call, not just latency.
2. **Give maintenance less disk time while restores are queued.** Local compaction
   currently has one stream per worker but no foreground-pressure scheduling.
   Add cooperative pacing with progress guarantees rather than dropping work or
   increasing concurrency. Account for both stream inactivity and the native
   request deadline so pacing does not turn completed work into timed-out,
   repeated exports. Preserve input pins, durable publication and adoption order.
3. **Amortize worker journal durability work.** Several synchronous journal commits
   per mount are slow under shared disk pressure. First separate lock wait,
   transaction and fsync timings. Batch independent volume operations with
   per-operation rollback/fencing and acknowledge only after the shared commit
   is durable. Moving worker authority to a remote database or disabling fsync
   is not justified by these measurements.

Do not raise wake concurrency based on these samples: workers already have
filesystem and I/O contention. None of the larger candidates above is implemented
or performance-qualified by this change.

Evidence: [storage phases](../benchmarks/storage-contention-2026-09-22/storage-phases.txt),
[storage counters](../benchmarks/storage-contention-2026-09-22/storage-metrics.txt),
[process I/O](../benchmarks/storage-contention-2026-09-22/io-source.txt),
[Linux tests](../benchmarks/storage-contention-2026-09-22/linux-tests.txt),
[baseline failure](../benchmarks/storage-contention-2026-09-22/baseline-regression.txt).
