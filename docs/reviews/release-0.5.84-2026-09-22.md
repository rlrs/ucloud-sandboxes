# Gateway contention and failed 256-sandbox qualification

Runtime commit `239ab61`, deployed 2026-09-22 21:29:38 UTC. Production was idle
before the restart. Gateway, relay and autoscaler were confirmed active afterward.
Postgres relay authority and the storage backend were unchanged.

The preceding application run had response-ready-to-wake-completed median
24.74 seconds, p95 93.84 seconds, maximum 176.50 seconds in the final five-minute
sample. Gateway profiling showed a create holding placement while waiting for
routing, many queued lifecycle writes, concurrent fleet scans, and blocked
heartbeat reconciliation. Later, after the workload ended, the writer queue was
empty; the accumulated wait time is not evidence of a permanent deadlock.

## Implemented and isolated verification

Full fleet routing scans now queue separately from single-sandbox reads and
lifecycle writes. Inventory reconciliation and create allocation use their atomic
SQL transaction without also holding the fleet projection lock. No cached/stale
route snapshot, weaker ownership fence, or client rejection threshold was added.

The Linux reproduction mixes 24 fleet readers, 32 lifecycle writers and two
inventory reconcilers over 128 routes containing 16 KiB specifications. Lifecycle
write p95 fell from 1.279 to 0.104 seconds; inventory reconciliation p95 fell from
2.197 to 0.083 seconds. This is a component result, not a wake SLO result.
161 routing/lifecycle/gateway tests and six load-harness tests passed on Linux.

## Production test: correctness passed, performance failed

The driver ran on `rasmus-dev`, separate from the gateway, using the staged
0.4.23 SDK source. The initial launch found an older SDK on the driver and exited
before requesting capacity; it was corrected before the reported run.

`relay-load-14f536c61f8d` created 256 sandboxes, each with 512 MiB incompressible
resident memory, 128 MiB dirtied each cycle, filesystem changes, and guest tool
execution. Three cycles used model waits of 20–25 seconds and 24 concurrent
fleet pollers. All 768 cycles and cleanup completed without errors. The 512
post-warmup observations had:

- Commit-and-wake p95 **38.416 seconds**.
- Response-ready-to-usable execution p95 **47.988 seconds**.
- Actual parking observed in **408 of 512** measured cycles.

This is unequivocally a failed performance qualification. A diagnostic eight-second
stack profile ran during the workload, so these are not profiler-free timing
claims or an apples-to-apples comparison with the application run.

All 256 routes initially landed on workers 12399461 and 12399462 (121/135).
Workers 12399463 and 12399464 became ready later but remained idle. The busy
workers showed heavy I/O/reclaim and internal pressure retries. A sample recorded
81 memory-pressure retry events across 81 requests, despite high MemAvailable.
MemAvailable included substantial file cache; it must not be treated as proof
that reclaim is free. No storage error volumes were reported.

The profile shows remaining placement serialization and worker proxy waits.
The fixed 15-second retention cap also forced checkpoints for model responses
expected only a few seconds later. Subsequent work must address this and qualify
mixed provisioning/lifecycle traffic, rather than treating a smaller steady-state
benchmark as proof that production is healthy.

Artifacts: [release evidence](../benchmarks/release-0.5.84-2026-09-22).
The successful fresh worker boots used 0.5.84. All non-agent bundle files and
non-product Python dependencies were byte-identical to the qualified prior bundle.
The two legacy workers retired at the user's request earlier this evening are no
longer running; the earlier 0.5.83 report describes their pre-retirement state.
