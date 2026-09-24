# Placement contention: local reproduction and fixes

No production access, writes, configuration changes, restarts or deployments were
performed for this investigation. The starting evidence is the previously
collected production observation of approximately 22-second placement p95 and
substantial gateway CPU pressure. That histogram includes waiting; it does not
establish 22 seconds of computation by a single placement.

## Reproduced bottleneck

Same-owner wake admission reads all routes belonging to an owner while holding
the shared placement reservation. Its SQLite cursor previously stepped through
one row at a time. sqlite3 releases and reacquires the GIL on these calls, letting
other CPU-busy request threads repeatedly delay a globally serialized operation.
Fleet heartbeat reads likewise stepped through worker rows and opened an
explicit read transaction with additional file-permission checks.

The local benchmark at
`docs/benchmarks/gateway-cpu-2026-09-24/placement-read-contention.py` creates 320
routes over ten nodes and reads 32 owner routes and ten heartbeats. The prior
reader is included as the baseline. Two ordinary Python CPU threads reproduce
contention without production traffic or network calls. Median results over 12
reads per implementation:

| Read | Before | After |
| --- | ---: | ---: |
| Owner routes, idle | 0.956 ms | 0.981 ms |
| Owner routes, two CPU competitors | 66.597 ms | 31.183 ms |
| Heartbeats, idle | 0.511 ms | 0.568 ms |
| Heartbeats, two CPU competitors | 70.889 ms | 24.764 ms |

These are synthetic local wall-time measurements, not Linux production latency
qualification. They demonstrate the convoy mechanism and reductions of 53% and
65% under this contention shape; they do not prove all observed 22-second waits
are explained or eliminated.

## Changes

- Owner-scoped route reads use the same SQL JSON aggregation and canonical row
  decoder as fleet reads. The result is fetched once and decoded after returning
  the connection; identity matching, ordering and row validation are preserved.
- Public heartbeat snapshots use one coherent SELECT without a read transaction,
  with decoding outside the pooled connection. Canonical payload validation,
  cross-worker binding checks, quarantine projection, and caller-owned copies
  are preserved. Write transactions retain their existing path.
- Migration selection indexes routes by worker, evaluates each candidate's
  available resources once, and reuses that result for ranking. It no longer
  scans the whole fleet for each candidate or recalculates the selected ranking
  values. There is no persistent admission cache.
- Wake reservation traces separate process-lock wait, file-lock wait and lock
  hold. Creates record hold duration alongside existing read/scoring/commit
  phase timings. This supports further attribution when production observation
  is authorized again.

281 local relay dispatch, routing, registry, control-state, fleet, placement, wake, migration and
consolidation tests pass. Additional regressions cover ownership alias matching
without duplicates, externally modified rows, invalid JSON, heartbeat snapshot
transaction count, mutable-copy isolation, quarantine, and one resource
calculation per migration candidate. Ruff and diff checks pass. Changes remain
uncommitted and undeployed.

## Response delivery and wake queue follow-up

The retained production snapshot had 93 pending deliveries (oldest 6.835 seconds)
and 582 unfinished park operations. Unfinished operations include deferred work
and active claims: they are not all runnable queue entries. Completed wake p95
was 0.405 seconds over five minutes, but that excludes still-pending deliveries
and hid a 46.98-second maximum. The approximately 49-second `relay.wait_for_worker`
measurement includes model inference and is not a wake-only measurement.

An unclaimed park operation previously survived model completion until another
dispatcher claimed it, skipped the obsolete notifier, and committed completion.
Deferred parks could wait for their retry deadline first. Model completion now
retires unclaimed parks in the same transaction that commits the response and
creates the wake intent. Claimed parks remain fenced and can still deliver late
transport receipts; this does not cancel a park already in progress. This removes
unnecessary dispatch and database work without changing concurrency limits.

Lifecycle stats now partition unfinished work into due, deferred and claimed
counts, and report oldest due age and entries with a previous error. Dispatch
spans record eligible-to-claim delay, claim-to-dispatch delay, committed-response
age at dispatch, attempt count, and requested retry delay. The span surrounds the
notifier and durable completion, parenting the existing downstream HTTP trace.
These distinguish queue delay from gateway/worker execution when deployed.

This investigation has not established which stage caused each production tail.
The new diagnostics and cleanup are local changes, not evidence that production
latency has improved. No production access or deployment occurred in this follow-up.

Validation used isolated local PostgreSQL 17 with a UTF-8 database and Python
3.14.3: the full relay/shared-control suite ran 93 tests, with two SDK-dependent
tests skipped initially. Both skipped cases passed separately with the current
SDK source on the import path. Coverage includes atomic result/wake rollback,
deferred park retirement, late transport receipts, saturated park dispatch,
lost notifications, restart recovery, and queue-state accounting. The 281 local
gateway/lifecycle regression tests also pass, as do Ruff and diff checks.

An initial full run on local Python 3.10 stalled in the existing idle-poller
cancellation test; it passed on Python 3.14, matching production's Python series.
That does not establish Python 3.10 compatibility. All measurements here remain
local macOS qualification, not a Linux load test or production latency result.
