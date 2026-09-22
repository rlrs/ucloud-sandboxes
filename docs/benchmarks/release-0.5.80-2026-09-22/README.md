# Relay poll transaction reduction, 2026-09-22

The worker poll path previously committed a separate heartbeat transaction,
then performed separate candidate selection, lease update and payload reload
queries. Release 0.5.80 commits the heartbeat with the claim and returns leased
payloads from one CTE statement. Registration locks, SKIP LOCKED claims, unique
lease tokens and atomic rollback remain in place. The lease clock is read after
the heartbeat upsert, so contention on the heartbeat cannot consume the lease.

## Linux microbenchmark

`poll-microbench.py` compares the baseline relay module from c714325 with the
candidate on real PostgreSQL 17. Each case uses 64 concurrent simulated workers,
16 enqueue/poll/respond cycles each, 32 KiB requests, a fresh schema, and the
same 16-connection pool. Sequence is before/after/after/before. The client
container has two vCPUs; PostgreSQL runs separately, so this isolates SQL client
work and is not a production or colocated CPU benchmark.

| Metric | Before, two runs | After, two runs |
|---|---:|---:|
| Client process CPU | 1.994 / 2.008 s | 1.697 / 1.696 s |
| Total wall time | 2.916 / 3.087 s | 2.760 / 2.778 s |
| Poll p95 | 92 / 99 ms | 65 / 64 ms |
| Separate heartbeat commits | 1,024 each | 0 |

About 15% less client CPU and 32% lower poll p95 in this isolated workload.
These are not predictions of end-to-end wake improvements.

## Validation

The Linux suite passed 1,209 tests, with ten environment-dependent skips.
Tests include competing claimers, registration replacement, distinct leases,
batch ordering, heartbeat/claim atomicity, and rollback after hydration failure.
The built wheel installed and verified successfully in a clean Linux venv.

Production qualification results will be recorded after deployment.
