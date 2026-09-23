# Relay control write amplification, 2026-09-23

This candidate removes two measured sources of relay control work while keeping
synchronous durable result acceptance and exact lifecycle ownership fences.
It does not change PostgreSQL durability settings, pool size, lifecycle lease
length, or renewal cadence. Production acceptance is still required.

## Production evidence

The retained VictoriaMetrics queries cover 15:50–16:00 UTC on September 23.
They overlap the end of pressure run `relay-load-e46611368211` and the following
sustained baseline. These are cumulative seconds across concurrent transaction
samples, not wall-clock elapsed time; telemetry arrival and window boundaries
limit exact attribution to either run.

- Per-operation lifecycle renewals accumulated 4,819.6 seconds of failed pool wait
  and 1,769.1 seconds of successful pool wait.
- Tunnel authorization accumulated 538.5 seconds of successful commit wait and
  3,439.2 seconds of pool wait.

The earlier [direct WAL observation](../relay-pool-pressure-2026-09-23/README.md)
proved that virtual-disk durability stalls can hold all sixteen connections.
Reducing redundant writers and queued renewal transactions reduces amplification;
it does not prove that the underlying storage latency is fixed.

## Candidate behavior and ownership

Tunnel authorization previously used `SELECT FOR SHARE`. PostgreSQL assigns a
transaction ID and writes tuple-lock WAL for that operation, even though the
lock is released before the HTTP body is read. The preliminary credential check
now uses an ordinary read. Enqueue checks the expected registration token inside
its existing locked transaction, before accepting work. A registration replaced
between precheck and enqueue cannot authorize a request against its replacement.
Bearer-authenticated endpoints retain their existing registration behavior.

Active park/wake claims now share one renewal loop instead of one task and
transaction per dispatch. Each batch retains the exact request/action/claim-token
predicate and ignores completed or replaced claims. Rows lock in a consistent
order. Ownership hints are pruned only after a successful commit; cancellation
and ambiguous commit outcomes cannot complete lifecycle work. A previous dispatch
cannot remove a replacement dispatch's claim. Renewal cadence is unchanged at
one third of the existing lease duration; a newly started operation waits at most
that interval before its first renewal. Restart recovery still uses durable
claim expiry and the original idempotent lifecycle identity.

## Isolated Linux measurement

`benchmark.py` runs against a caller-supplied test DSN and creates/deletes only
its uniquely named test schema. It uses the actual candidate database/state
classes, with the prior authorization and per-claim renewal SQL retained as the
baseline. This ABBA comparison ran on Linux, Python 3.13.2, PostgreSQL 17, with
512 items per sample and the unchanged sixteen-connection pool.

| Work | Baseline seconds | Candidate seconds | Transactions per sample | WAL bytes |
| --- | --- | --- | --- | --- |
| 512 sequential authorizations | 0.899 / 0.910 | 0.1216 / 0.1217 | 512 → 512 | 49,296 → 0 |
| 512 concurrent claim renewals | 0.150 / 0.132 | 0.00984 / 0.00944 | 512 → 1 | 91,696–109,760 → 167,664–170,392 |

Authorization became approximately 7.4× faster here. Batched renewal became
approximately 14–15× faster; summed pool wait fell from 32–41 seconds to about
9 microseconds, and client Python CPU fell from 82–97 ms to 1.7–1.9 ms.

**Renewal WAL volume increased** in this fixture due to the batch/ordered-lock
update shape. The benefit is fewer durable transactions, round trips, Python
tasks, and queued borrowers—not a claim of lower renewal byte volume. PostgreSQL
group commit means transaction count is not an exact physical-fsync count.
WAL bytes use cluster-global LSN differences, so unrelated writes can affect
measurements; authorization's two zero-WAL samples and transaction counts are
also supported by explicit read-only and transaction-observer tests. No fleet
latency extrapolation is made from this component benchmark.

## Verification

The isolated real-PostgreSQL relay suite passed **64 tests in 16.406 seconds**.
New cases cover read-only authorization without an assigned transaction ID,
registration replacement between precheck and enqueue, 512 renewals in one
transaction, partial batch ownership loss, already-completed claims,
cancellation, an acknowledged-lost commit, and renewal of live work while a
peer process attempts takeover. Existing process-replacement tests exercise
recovery after shutdown. The model-relay and async lifecycle transport suite
also passed **26 tests in 1.453 seconds**. Independent source review found no
blocker. No production service was changed for this qualification.
