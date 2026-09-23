# Relay empty-poll write amplification

The rc19 pressure run `relay-load-70fe49fe7db7` ran from 16:59:15 to
17:03:19 UTC on 2026-09-23. The existing one-second PostgreSQL observer found
one initial WAL synchronization stall, reaching 3.68 seconds with 15 other
backends waiting on WALWrite. The pool recovered; no transaction older than
0.5 seconds was sampled later. This differs from rc18's observed 34-second WAL
stall. It does **not** explain rc19's later worker startup timeout or long wake
latency.

The retained VictoriaMetrics query nevertheless showed avoidable work:
14,660 inference-claim transactions, 98.9 summed seconds in their commits and
274.4 summed seconds waiting for the pool. Empty SDK polls updated worker
heartbeats and locked the registration even without available work. Sum values
are concurrent work across requests, not wall-clock run duration.

## Change and guarantees

There is one transaction per poll iteration. Its first ordinary SELECT validates
registration and observes available/expired work plus heartbeat freshness. A
fresh empty poll takes no row lock, assigns no transaction ID and writes no WAL.
Only ready work or a due heartbeat enters the existing SHARE-fenced mutation
path; the token is revalidated atomically before leasing work.

Poll heartbeat writes are coalesced within one third of the shorter existing
worker-retention and requested inference-lease lifetime (200 seconds with default
3600/600-second settings). Worker rows are used by statistics and retention,
not request authority, autoscaling or health freshness decisions. Explicit
`/worker/heartbeat` remains immediate. A long outstanding poll refreshes when
its heartbeat becomes due. Concurrent peers use one transaction advisory gate
and re-read freshness; an empty loser does not acquire a row lock or execute a
no-op UPSERT. No process-local authentication cache is introduced.

Queue notifications and the existing half-second durable readiness fallback
remain hints. Missed notifications, revoked registrations, expired leases,
transaction rollback and ambiguous response commits retain their existing
handling.

## Qualification

68 real PostgreSQL tests passed in 17.371 seconds. New tests prove empty polls
work under `SET TRANSACTION READ ONLY` without an assigned transaction ID,
registration replacement between observation and claim cannot grant a stale
lease, eight simultaneous peers produce one due heartbeat write, and a long
poll refreshes within its lease-derived interval. Existing claim rollback,
missing-hint recovery, lease expiration, HTTP, cancellation and restart tests
also pass.

The standalone `benchmark.py` retains the rc19 poll method as its baseline and
uses a disposable PostgreSQL schema. It compares ABBA order with 512 separate
registrations and 128 concurrent polls, first empty and then with identical
available request bodies. Run only against an isolated qualification database
using `UCLOUD_TEST_POSTGRES_DSN`; it never targets production. No other
qualification workload ran during the retained ABBA measurement.

| Case | rc19 baseline | Candidate |
|---|---:|---:|
| Empty polls, elapsed | 0.251–0.257 s | 0.090–0.095 s |
| Empty polls, Python CPU | 0.190–0.214 s | 0.090–0.094 s |
| Empty polls, global WAL delta | 125,544–151,904 B | 0–448 B |
| Ready claims, elapsed | 0.230–0.245 s | 0.228–0.234 s |
| Ready claims, global WAL delta | 515,000–520,808 B | 440,760–445,992 B |

Both versions use 512 transactions in each case. Empty candidate transactions
are read-only. WAL is a cluster-global LSN delta, so the small nonzero residual
cannot be attributed exclusively to the measured method; the transaction-ID
and READ ONLY tests establish that the empty candidate path performs no durable
writes. Ready claims retain synchronous durable commits. These component results
are not a claim that production pressure performance or the wake SLO passes.

`production-pg-summary.json` contains bounded, sanitized observer evidence;
`production-operation-metrics.jsonl` records exact query/interval/units;
`abba.jsonl` contains the unprofiled component measurements. Raw observer data
was retained locally under `/private/tmp/rc19-pg-evidence`.
