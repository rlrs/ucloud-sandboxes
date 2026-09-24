# Minimal overload corrections — 24 September 2026

Scope: three runtime files; no worker-cap increase, database-pool expansion,
timeout extension, or restoration of the deferred memory changes.

## Overnight evidence

Reviewed production between 23:00–00:00 UTC, 23–24 September (01:00–02:00
Copenhagen). The fleet had six ready workers at 23:17, seven at 23:19:34,
and eight at 23:33:46. At 23:34:28 and 23:34:47 the autoscaler reported
107 and 214 GiB of additional forecast RAM demand and could not provision
because `max_nodes=8`. Those figures include prepared reservations, not just
resident memory. By 23:40 the forecast was below fleet capacity, but the final
active five-minute interval still had approximately 15-second worker wake p95.
Capacity constrained the burst; insufficient node count does not explain every
subsequent delay.

Histogram estimates for the hour were 105.7-second park p95 and 15.3-second
worker wake p95. Checkpoint capture dominated parking. Sampled failed captures
hit the existing 60-second native timeout; histogram interpolation is not an
exact maximum. There were 95 successful and 24 failed park observations.

Relay logs recorded 76 pool-acquisition timeouts between 23:34:00 and 23:34:44,
alongside failed delivery reads and lifecycle claims. Unregister requests let
pre-transaction admission failures escape as unhandled errors. This evidence
does not identify why connections remained occupied; normal transaction p95
does not rule out short commit, lock, scheduling or storage stalls.

## Corrections

1. Under the existing high-I/O-pressure threshold, resident reclamation lets
   in-flight work drain before admitting more captures. With no reclaim in
   flight, one candidate can still progress. Healthy storage retains measured
   byte-based parallelism. Existing lifecycle fences and settle windows remain.
2. Lease renewal and rollout unregister return the existing retryable database
   admission response when no transaction was entered. Both operations use one
   transaction. This does not classify ambiguous model requests, acquired
   transaction failures, or commit failures as safe HTTP retries.
3. Heartbeat receipt converts the observed SQLite lock-contention error into
   retryable backpressure. Four overnight writes were otherwise reported as
   unreadable control state. Other storage errors retain their existing handling.

These are overload mitigations, not proof of subsecond pressure performance or
a cure for the underlying database stall. Production deployment and matching
load measurement are separate from the targeted regression checks.

## Validation

The final combined gate passed **128 Linux tests in 140.372 seconds**, using a
separate PostgreSQL container and the released SDK source. Tests cover exhausted
pool admission and safe retries, preservation of ambiguous model-call semantics,
checkpoint progress under high I/O pressure, recovery of measured parallelism,
and heartbeat recovery after SQLite contention. Two pre-existing test timing
assumptions were corrected: the 100 ms acquisition deadline now applies after
pool startup, and lifecycle polling waits for the durable callback completion.
Production timeouts are unchanged.

All six tested code/test files matched their recorded hashes. The temporary test
database was removed and production services remained active. See the
[qualification record](../benchmarks/minimal-overload-2026-09-24/qualification.json).
These changes have not been deployed or measured under production load.
