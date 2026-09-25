# Routing authority deployment and native qualification

Production routing moved to PostgreSQL on 25 September 2026. The gateway no
longer takes a fleet-wide process/file placement lock on this backend. Public
creates and explicit wakes enter a durable lifecycle queue; the private placement
service executes the existing routing domain with worker capacity revision fences.
The implementation is described in [placement authority](../../placement-authority.md).

The initial rc32 deployment imported and hash-verified 691,963 rows across 14
tables. Routing, generations, migrations, program state, exec routes, demand and
snapshot liveness moved together. The SQLite backup is retained, not an active
fallback. [Deployment receipt](deployment.json).

## Qualification boundaries

The canonical repository benchmark uses 512 admissions over eight simulated
worker records. On the Linux gateway it completed in 2.145 seconds, p95 201 ms,
without overbooking or transaction retries. These are database admission timings,
not guest wake timings. [Raw result](canonical-linux-512.json).

The native workload runs from a separate Linux driver through the public gateway
and relay using SDK 0.4.30. Each sandbox runs a managed Python agent, retains
128 MiB, dirties 16 MiB per cycle, writes 64 files of 64 KiB, performs 100 ms CPU
work, and waits for a simulated model response for 10–15 seconds. Each response
must produce a guest-originated continuation receipt, verified tool execution,
and memory/file integrity checks. Sandboxes request one vCPU, 1 GiB RAM and
4 GiB disk. Provisioning concurrency is 32. The first cycle is excluded from
headline latency; per-phase summaries retain warmup. Reports include driver lag,
worker resources, health checks and cleanup failures.

Natural parking follows production policy. A warm continuation is not a full
checkpoint restore: the rolling rc32 runs observed no fully parked measured
cycles. Forced parking is reported separately and includes waiting for parking
when it delays an already-ready model response. The acceptance thresholds remain
0.8 seconds for continuation and 1 second for verified usable execution; failed
thresholds have not been relaxed.

## rc32 results

| Workload | Completed cycles | Correctness | Continuation p95 | Usable execution p95 |
| --- | ---: | --- | ---: | ---: |
| [Natural 16, 3 cycles](native/native16.json.gz) | 48 | Pass | 0.224 s | 0.437 s |
| [Forced parking 64, 4 cycles](native/forced64.json.gz) | 256 | Pass | 3.956 s | 5.131 s |
| [Rolling natural 256, 5 cycles](native/rolling256.json.gz) | 1,280 | Pass | 0.841 s | 1.233 s |
| [Rolling natural 512, 5 cycles, scale-up](native/rolling512-before.json.gz) | 2,560 | Pass | 6.149 s | 7.233 s |
| [Rolling natural 512, second run](native/rolling512-warm-before.json.gz) | 2,560 | Pass | 7.907 s | 10.011 s |

Full reports are retained as compressed JSON in [native](native/); decompress with `gzip -dc FILE.json.gz`. The 256 and both 512 rolling runs passed
fleet-health checks with no workload or cleanup errors. The forced-64 run had one
failed resource probe that recovered; its separate fleet-health gate failed.
Only the natural-16 run passed the full latency gate.

The 512 runs expose contention rather than demonstrate a completed latency goal.
Observed worker advisory waits reached 1.4 seconds. Program transitions held
worker capacity revision conflicts for 37–162 ms despite not changing aggregate
capacity. Sampled WAL waits were only 1–3 ms. The second run used eight workers,
including two added during the test; it is not an identical-fleet comparison with
the first run's six workers. No production worker sizing or spending cap changed.

### Separate startup barrier failure

[Barrier 256](native/barrier256.json.gz) failed before model traffic: only 237
primaries started. Three workers retained approximately 67–68 GiB of future
growth reservations and another 1 GiB pending each, while the barrier prevented
started agents from reaching their first safe model wait. Physical memory and CPU
were not exhausted. Three agent-start requests timed out; the harness cancelled
the remaining tasks and cleaned its resources.

Rolling startup avoids that barrier, but does not fix simultaneous-start capacity
accounting. A correct follow-up needs incarnation-fenced evidence distinguishing
unlaunched, initial-growth, safe-wait and terminal states, with residual startup
bytes and whether existing transition totals already charge them. Placement can
then reserve startup exposure without double counting, release it after a proven
safe wait, and expose excess startup demand to autoscaling. Charging every active
sandbox's full shape permanently would sacrifice steady-state density; relaxing
worker memory/backing safety would reintroduce SIGBUS risk.

### Separate forced-parking bottleneck

[Curated attribution](native/forced64.analysis.json) has partial trace coverage,
explicitly reported. The worker used a median 4.9 of 32 cores with about 83 GiB
available RAM, but median I/O pressure was 60%. Two measured wakes spent
719–790 ms syncing retained memory backing. Sampled commit-to-worker-start
averaged 97 ms; worker wake averaged 811 ms. Subsequent guest/tool execution also
contributed latency. This is not solved by removing gateway serialization.

## Targeted capacity-fence refinement

The deployed rc33 release retains worker revision writes for capacity, owner,
incarnation, parked/waking state, snapshot and migration changes. It skips them
for unchanged program metadata, changes within nonterminal program membership,
and running confirmations that only advance activity/freshness. Active program
membership edges remain fenced, including against cold detach. Stale program
receipts resolve the canonical current owner before fencing.

Seven new real-PostgreSQL tests cover revision edges, progress while the worker
revision row is deliberately locked, running versus parked freshness, current
owner after migration and a concurrent cold-detach race. The original capacity
and ownership contract suites remain required.

Rc33 code commit `94f5537` passed 327 selected Linux tests against a separate
PostgreSQL 17 instance before deployment. The gateway HTTP qualification fixture
now reuses one pool per path, matching production; its 48-concurrent-lookup test
prevents regression. The temporary test database was stopped before load testing.
[Deployment receipt](deployment-rc33.json). SDK and worker native binaries did not
change. Existing routing authority remained in place during this code upgrade.

[Rc33's cold repeat](native/rolling512-after.json.gz) completed all 2,560 cycles correctly on seven workers. The
measured continuation p95 was 5.726 seconds and usable-exec p95 7.367 seconds.
An exact join of all wake command and relay request identities attributed p95
1.450 seconds before queue creation, 1.281 seconds inside the queue, and
3.031 seconds after queue completion before relay acknowledgment. These stage
percentiles must not be added together.

The [subsequent warm repeat](native/rolling512-warm-after-profiled.json.gz) also passed correctness but was instrumented with
15-second gateway and relay CPU profiles. The gateway profiler reported sampling
lag; treat this run as attribution evidence, not an uninstrumented performance
comparison. Its measured usable-exec p95 was 14.227 seconds. Public gateway work
was spread across synchronous request handling and asynchronous I/O, with no
single dominant leaf function. Exact paired stages and compressed profiles are
retained alongside the report.

## Completion-read isolation

Deployed rc34 gives the single batched completion poller a dedicated
one-connection pool. It reads the same durable command rows at the same cadence,
but cannot queue behind new submissions in their connection pool. Startup
failure recovery recreates unopened connections before any work is accepted;
explicit shutdown is terminal and cannot resurrect socket waiters after an
in-flight submission returns. Tests deliberately saturate the submission pool,
interrupt initial opening and race shutdown with successful submission.

Queue pool/body/commit timings reuse the relay's existing PostgreSQL metric.
Relay lifecycle HTTP durations are recorded separately from durable completion,
including when traces are unsampled. This resolves the ambiguity between
post-placement result delivery and relay completion transactions without adding
another telemetry system.

Rc34 (`fdd7b55`) passed 406 selected Linux tests against isolated PostgreSQL 17.
Its unprofiled rolling 512 run completed all 2,560 cycles with no workload or
cleanup errors and passed fleet health. No measured cycle fully parked, so this measures warm
continuation under the natural parking policy. Continuation p95 was 5.705 seconds;
usable execution p95 was 7.321 seconds. These miss the unchanged latency gate.
Ten workers were observed during this run; fleet composition differs from prior
runs, so this is not a controlled estimate of the isolated pool change's benefit.
[Deployment receipt](deployment-rc34.json),
[full workload](native/rolling512-rc34.json.gz),
[paired attribution](native/rc34-handoff-attribution.json).

All 2,560 wake identities matched. Before-queue p95 was 2.229 seconds, queue
execution 1.064 seconds, and completed-command to relay acknowledgment 3.231
seconds. Do not add these percentiles. At peak, enqueue pool wait p95 was about
2.2 seconds. Dedicated result-pool wait stayed below 1 ms, but result transaction
p95 reached about 0.67 seconds. Isolating the result connection resolves its pool
starvation; submission and response scheduling remain bottlenecks. Comparing
against the profiled rc33 run would overstate the evidence of improvement.

## Single-statement route reads

Rc35 uses one autocommit SELECT for an exact route lookup outside a transaction,
removing BEGIN, isolation setup and COMMIT from that path. Canonical SQL and row
decoding are shared with transactional reads. Inside placement, reads reuse the
existing connection, snapshot and uncommitted writes; multi-query readers retain
repeatable-read semantics. Seven focused regressions cover protocol call count,
nested writes, concurrent commit visibility, deletion/recreation, authority
replacement, failed-query recovery and coherent multi-query snapshots.

Rc35 (`42b6b78`) passed 413 selected Linux tests using isolated PostgreSQL 17
before its idle production deployment at 09:28:45 UTC. Gateway, placement, relay
and autoscaler health checks passed. The routing authority did not change again;
SDK remains 0.4.30, and native/OS/storage artifacts retain their prior byte-level
closure. [Deployment receipt](deployment-rc35.json). The isolated qualification
database was stopped before the native test.

The final [rc35 rolling 512 run](native/rolling512-rc35.json.gz) completed all
2,560 cycles correctly, with no workload or cleanup errors and healthy workers.
It used four workers after a cold scale-up (173/157/92/90 sandboxes), versus ten
observed during rc34. The temporary capacity reservation did not preserve the
same warm fleet across releases. This is not a controlled latency comparison.
Continuation p95 was 6.480 seconds and usable execution p95 8.603 seconds, missing
the unchanged latency gate. No measured cycle fully parked. The system passes
this workload's correctness checks; subsecond high-load performance is not qualified.

[Exact paired attribution](native/rc35-handoff-attribution.json) covers all 2,560
wakes: before-queue p95 1.357 seconds, placement 2.189 seconds, and completed-command
to relay acknowledgment 2.724 seconds. Peak enqueue and result pool wait p95 were
below 1 ms; worker-specific placement contention was observed during concurrent
creates on the denser fleet. These percentiles must not be summed. Further
performance work is deferred; no additional tuning or capacity policy changes
were made after this qualification.

Final cleanup left zero sandbox routes, queued commands and pending demand.
Gateway, private placement and relay health returned HTTP 200 in approximately
12 ms, 2 ms and 2 ms respectively. Temporary test credentials and capacity
reservations were removed; the isolated local and remote qualification databases
were stopped. At this closeout, production ran rc35 with the ten-worker cap unchanged.

## Remaining architecture boundaries

Multiple unrestricted public gateway processes are not yet qualified. Image-build
dispatch, external migration execution and multi-step registry dependency changes
still use process-local coordination. PostgreSQL routing alone does not make
those sequences safe across processes. Moving those mutations behind a single
coordinator or giving them cross-process ownership is required before enabling
multiple public request processes. Connection and upload budgets also need to be
allocated across processes rather than multiplied accidentally.

## Opus follow-up: rc36

Commit `1020d45` (Opus) deployed rc36 at 11:29:53 UTC. It adds a direct warm-wake
path with generation fencing and queue fallback, isolated placement queue I/O,
single-statement enqueue/results, per-worker turns before connection acquisition,
and in-flight create ranking plus reuse of unchanged placement data. All 475
selected Linux tests passed. All four services are active; a missing-sandbox wake
traversed the durable queue and returned its expected 404 in 70 ms, leaving no
queued work. This is an idle functional smoke test, not a new load-performance
result. SDK remains 0.4.30. [Deployment receipt](deployment-rc36.json).

## Opus follow-up: rc37

Commit `6e99d19` (Opus) deployed rc37 at 13:18:31 UTC. It isolates relay delivery
and lifecycle transactions, shortens heartbeat row-lock ownership, reduces exec
lookup round trips and removes growth commits from the worker-wide capacity guard.
Provisional growth remains charged while commits finish. A race correction keeps
the covered-growth check and wake fence ordered against per-sandbox safe waits.
All 577 selected Linux tests passed, including the new race regression. Production
health and a queued-wake smoke check passed; the relay delivery pool is active.
There were no live workers to upgrade; new workers use the rc37 bundle. No native
load benchmark was repeated. [Deployment receipt](deployment-rc37.json).
