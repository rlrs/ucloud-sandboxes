# Shared-control qualification, 21 September 2026

This qualifies the first PostgreSQL result/wake implementation, not the complete
production backend. See [implementation scope](../../shared-control-qualification.md)
and [system design](../../shared-control-plane-design.md).

All reported trials completed their response/state checks. These are local
macOS tests with PostgreSQL 17, four logical nodes, two dispatchers/pools with
16 connections each, 32-KiB model responses, durable commits and simulated worker
callbacks. Database durability settings and the crash test result are retained in
[validation.json](validation.json).

| Workload | Current SQLite sequence p95 | PostgreSQL slice p95 | Trials per backend |
| --- | ---: | ---: | ---: |
| [256 simultaneous results](burst-256.json), no restore delay | 1.61–1.66 s | 0.54–0.55 s | 3 |
| [512 simultaneous results](burst-512.json), no restore delay | 3.03–3.33 s | 1.00–1.03 s | 3 |
| [512 results at 41/s](steady-512.json), simulated 600-ms restore | 0.620 s | 0.644–0.650 s | 2 |

The burst advantage supports pursuing the shared transaction/queue design.
It does not establish universal PostgreSQL superiority: the simplified SQLite
reference is slightly faster at this steady arrival rate. The 512-result burst
also consumes about a second in coordination alone, without real restoration.
The product's subsecond loaded-wake goal remains unqualified.

The initial dispatcher waited for an entire batch before claiming more work.
Its [rejected steady-load run](rejected-batch-dispatch.json) had PostgreSQL p95
1.19–1.20 seconds. Refilling individual slots removed that batch barrier; a
regression test proves a third operation starts while an earlier RPC is blocked.
Source hashes for that rejected prototype were discarded because edits for the
next candidate happened while it was running. Final report hashes are captured
before each benchmark run.

## Interpretation limits

The SQLite reference uses the repository's existing routing/relay persistence
methods, but omits HTTP, placement inventory and the relay's global async lock.
PostgreSQL implements a different, atomic result/operation protocol. This is a
comparison of coordination paths, not identical SQL on different engines.

No real sandbox was parked or restored, no worker storage was stressed, and no
production services were modified. Two PostgreSQL pools run inside one Python
process; multi-process correctness has separate contract tests, but multi-host
performance is not measured here. Production Linux, network RTT, synchronous
standby latency, real bodies and worker I/O still require qualification.

Timing samples separate pool wait, transaction duration, commit and locking-query
duration. Locking-query duration includes execution/round-trip cost and must not
be reported as pure server lock wait. At burst load, result pool waits and the
per-node reservation/completion transactions are visible costs. Commit p95 was
substantially smaller on this test storage. Raw per-operation quantiles are in
each report; component p95s must not be added together.

## Validation

- 1,089 server tests passed, with six pre-existing environment-dependent skips.
  This included 24 real PostgreSQL contract tests.
- The PostgreSQL tests cover duplicate/conflicting results, coalescing, expired
  leases/claims, rollback, independent-node progress, capacity queueing, stale
  proofs, deployment isolation, cancellation, lost acknowledgments, abrupt
  dispatcher process exit and continuous slot refill.
- An immediate stop/restart of the isolated PostgreSQL cluster preserved the
  committed model response, dispatching operation and restore reservation.
  Recovery reused that operation and released its reservation on valid proof.
- Ruff and diff checks passed. The wheel was built and its packaged SQL schema
  checked through an isolated installation.
- CI now runs the real PostgreSQL contract and a database kill/restart test.
  The added CI job has not yet run remotely.

The natural-mode live SDK harness is implemented and unit-tested, but has not
been rerun against production in this implementation turn.
