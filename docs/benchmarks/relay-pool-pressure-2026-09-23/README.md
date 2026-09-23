# Relay database pressure, 2026-09-23

The rc17 pressure run `relay-load-d0bf2463839e` failed at 14:15:43.707 UTC
while worker polling acquired a PostgreSQL connection. The uncaught
`psycopg_pool.PoolTimeout` produced an aiohttp HTTP 500. Retained relay/gateway
logs contain the same failure for result commits, authorization, registration,
and maintenance. They do not show an ambiguous commit being reported as a pool
acquisition timeout.

`rc17-metrics.jsonl` contains bounded VictoriaMetrics instant queries evaluated
at 14:17:00 UTC, with a three-minute `increase` window (14:14–14:17). The source
is the existing `ucloud_platform_postgres_duration_seconds_*` histogram, emitted
at transaction completion. Sums are cumulative seconds across concurrent
transactions, not elapsed wall time. Histograms estimate quantiles; observations
crossing the window boundary and telemetry delivery delay limit exact attribution.

| Completed samples | Body seconds | Commit seconds | Pool-wait seconds |
| --- | ---: | ---: | ---: |
| 5,079 successful | 102.66 | 1,266.17 | 1,968.39 |
| 248 failed | 20.06 | 158.64 | 2,158.59 |

Successful commit p95 was approximately 22.7 ms, so a long tail dominates the
commit total. Pool occupancy during intermittent commit stalls is a plausible
cause of the admission queue. These measurements do **not** distinguish database
WAL/filesystem latency from process or event-loop scheduling delay. PostgreSQL
WAL timing counters were disabled. No persistent connection leak was present
after cleanup: all 16 pool connections were idle, with no open transaction.

The data directory uses gateway `/dev/vda1`, ext4, with 234.4 GB available
(10% used); the WAL directory was 161 MB. Gateway kernel logs during
14:13:30–14:17 contained no clocksource gap. At 14:16:26 they recorded SYN
cookies on port 8092 and a balloon workqueue CPU warning. Worker clock gaps
are separate evidence, not proof of the cause of these PostgreSQL waits.
The benchmark also retained external ingress HTML `Job is unavailable` HTTP
503 responses; those are distinct from relay-generated errors.

## Narrow correction and next measurement

Connection acquisition failure now has a distinct internal exception. Only
worker poll and exact fenced response submission map it to HTTP 503
`relay_database_busy`, `retryable: true`, and `Retry-After: 1`. The server does
not retry a transaction. PoolTimeout after acquiring a connection, including
an exception after COMMIT, retains its original failure semantics. Model/tunnel
requests are excluded: a later transaction may fail after enqueue already
committed, so transaction admission alone does not prove safe HTTP replay.

Real PostgreSQL HTTP tests exercise exhausted-pool poll and response requests,
release and retry with one durable result, and duplicate result acknowledgment.
Additional regressions distinguish BEGIN/body/COMMIT failures and a model call
whose cancellation transaction fails after enqueue. This fixes error
classification, **not the underlying stall**. Pressure qualification is pending.

`collector.py` is an owned, bounded diagnostic command, staged on the gateway as
`/tmp/relay-pg-pressure-collector.py`. It uses one persistent read-only PostgreSQL
observer, a 750 ms statement timeout, 1 Hz aggregate state/wait-event/transaction
age, WAL/checkpoint counters, and host physical-disk/PSI counters. Every five
seconds it reads the existing relay stats API, retaining only pool counters;
that API itself can time out under pressure. It never prints SQL text,
parameters, registration identities, credentials, or full configuration.
A two-second idle smoke measured roughly 3–7 ms per database/host sample.
Run with the gateway's installed Python and an explicit 900-second duration;
it stops automatically. Use a new output file for each trial.

## rc18 direct observation

The unchanged pressure profile `relay-load-ca1cbcf81ed1` reproduced the stall.
`rc18-pressure-window.jsonl` retains the collector's 15:11–15:14:18 UTC window.
From 15:12:54 through 15:13:26, each one-second sample showed one PostgreSQL
backend waiting on `IO/WalSync` and fifteen on `LWLock/WALWrite`. The maximum
observed transaction age was 34.003 seconds. The separate read-only observer
continued to execute its queries in at most 7.18 ms throughout the retained
window. Thus the immediate pool bottleneck was WAL durability I/O, not a relay
event-loop stall or an expensive query holding these connections.

Between 15:12:53.020499 and 15:13:27.174258 (34.154 seconds), physical `/dev/vda`
completed just 0.464 MB/s of writes and 0.0014 MB/s of reads. It accumulated
32.939 seconds busy and 1,848.903 summed seconds waiting across completed write
requests. PostgreSQL WAL advanced approximately 483 KB and 26 syncs. Gateway
I/O PSI peaked above 60% full in this interval. This is severe virtual-disk
write latency at low completed throughput, not evidence of exhausted disk
space or bulk application throughput. Docker's PostgreSQL block-I/O caps and
all inspected system/docker `io.max` files were unset. No new gateway kernel
messages were present since 15:12:40.

By 15:14:14 all sixteen pool connections were available again, with no queued
waiter, after 270 cumulative acquisition errors. The backing storage cause
below the observed virtual disk is still unestablished; these data do not
identify a hypervisor, physical device, or provider fault. Increasing the pool
would not remove the serialized WAL durability wait. No durability setting was
weakened, and no production configuration was changed during this observation.

## rc18 correctness-budget repeat

The same pressure workload, with its request budget increased to 1,800 seconds,
ran as `relay-load-c4f46ab62793` and failed guest continuation for sandbox `0021`,
cycle 3. The guest-continuation check retained its original 180-second bound.
`rc18-correctness-pressure-window.jsonl` retains the aggregate collector through
cleanup. It recorded shorter WAL episodes around 15:29:28–32 and 15:29:59–15:30:03,
with a maximum sampled transaction age of 4.948 seconds. Observer queries stayed
below 5.4 ms. Pool error counters include cancellation, including this observer's
bounded stats request, so a counter increase alone is not proof of client HTTP
failure. The relay journal contained no error entries in the inspected
15:29:20–15:30:50 interval.

The retained single-request database record and sampled wake trace show a
separate progress problem:

- Request `662837034ec3407ba88a67cd4782cff2` committed its model result at
  15:27:42.412839 UTC.
- Gateway wake dispatch began 4.6 ms later, but its first worker attempt returned
  HTTP 503 after 48.65 seconds, at 15:28:31.070.
- PostgreSQL retained the wake obligation and made five attempts. The last was
  scheduled for 15:30:05.624; successful wake and delivery release were recorded
  at 15:30:42.720, 180.308 seconds after result commit and just after the test's
  continuation deadline. The transport epoch was unchanged.

The wake was marked successful, not released as a definitively unavailable
caller. Relay admission retries defer 1–5.25 seconds, and general failure backoff
is bounded at five seconds; no fast retry spin was observed. PostgreSQL had no
long wait in the first approximately 106 seconds after result commit. The later
short WAL stalls cannot explain the first 48.65-second worker rejection or the
whole continuation delay. Worker wake/growth admission requires the separate
progress investigation. Neither pressure correctness nor latency passes here.
