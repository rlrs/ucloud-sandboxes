# Managed-agent relay load, 21 September 2026

The subsecond loaded-wake target has **not** been met. These experiments establish
an independent reproducer and identify several costs that unit tests and empty
sandbox restores missed. All raw reports are retained, including slower and
confounded experiments.

See [the workload instructions](../../relay-load-benchmark.md). Tests used an idle
production deployment, release 0.5.74, with a driver outside the autoscaled fleet.
Each sandbox held 128 MiB of random memory, dirtied 16 MiB per cycle, rewrote 64
files, did CPU work, waited for a synthetic model response, actually parked, and
then verified memory, files, process identity, and tool execution. This is a
synthetic workload, not a capture of the user's completed production run.

`Wake` includes SDK response submission through relay commit/wake acknowledgment.
`Usable exec` extends that interval through a separate successful SDK exec that
observes the verified guest result. Both include driver/network/coordination
costs. The first cycle per agent is excluded from latency quantiles. The acceptance
criterion is the stricter usable-exec p95 below one second, with all cycles,
health probes and cleanup passing.

## Experiments

| Report | Agents / cycles | Wake p95 | Usable exec p95 | Result |
| --- | ---: | ---: | ---: | --- |
| [64-agent baseline](baseline-64.json) | 64 / 5 | 4.48 s | 6.99 s | Correct; SLO failed |
| [256-agent baseline](baseline-256.json) | 256 / 8 | 23.02 s | 28.27 s | All 2,048 cycles passed; one health timeout; SLO failed |
| [Worker registry changes](registry-candidate-256.json) | 256 / 8 | 17.35 s | 18.15 s | Correct; SLO failed |
| [Plus asynchronous relay wakes](async-candidate-256.json) | 256 / 4 | 17.88 s | 18.59 s | Correct; SLO failed |
| [Plus gateway admission changes](gateway-256.json) | 256 / 4 | 6.04 s | 8.05 s | Correct; SLO failed |
| [Plus connection reuse, replacement workers](pool-256.json) | 256 / 4 | 12.66 s | 14.93 s | Correct; slower; SLO failed |
| [Same-three-worker control](controlled-nopool-256.json) | 256 / 4 | 4.39 s | 5.55 s | Correct; SLO failed |
| [Same-three-worker connection reuse](controlled-pool-256.json) | 256 / 4 | 5.24 s | 7.16 s | Correct; no tail improvement; rejected |
| [16-slot experiment](slots16-confounded-256.json) | 256 / 4 | 5.87 s | 8.07 s | Correct; confounded; SLO failed |
| [Shadow observation reuse](shadow-confounded-256.json) | 256 / 8 | 5.23 s | 6.22 s | Correct; fourth worker joined unpatched; confounded |
| [Plus local writer coordination, verified fleet](writer-256.json) | 256 / 8 | 3.58 s | 4.56 s | All 2,048 cycles, health and cleanup passed; SLO failed |

The initial [four-agent smoke test](baseline-4c.json) predates the explicit parked
inventory observation and does not qualify loaded behavior.

These are sequential live runs, not randomized trials. The initial 256-agent
baseline placed 200/56 agents on two workers, with the second worker arriving
during startup. The registry, async and gateway runs used the same two workers
with approximately 138/118 placement. After idle scale-down, the pool experiment
used replacement workers. In the 16-slot experiment a third worker joined and
served 79 agents without the registry patch; only the two earlier workers had
their restore slots temporarily changed from the deployed **12** to 16. That run
cannot justify changing the production limit.

The final run used four workers with 77/68/64/47 agents. A startup gate verified
the worker candidate on every occupied worker before releasing model traffic;
the report includes that coverage evidence. Wake median was 2.66 seconds, p95
3.58 and p99 4.29; usable-exec median was 3.45 seconds, p95 4.56 and p99 5.30.
The difference from the initial two-worker baseline cannot be attributed solely
to code changes. The final writer change also lacks a matched four-worker control.

Wake timers exclude waiting for observed parking beyond the synthetic model
delay. In the final run that additional wait had a median of approximately
7.21 seconds and p95 10.36 seconds. It includes observation timing and is not a
worker checkpoint-duration measurement, but it is a substantial cost outside the
reported wake interval. The harness deliberately forces park/restore rather than
allowing a ready response to cancel parking; it does not qualify complete
model-ready-to-tool latency.

Earlier driver versions rewrote the complete JSON report on every event. The
later controlled pair uses one-second report checkpoints and a complete event
log, avoiding quadratic reporting work on the driver's event loop. Later reports
record the harness SHA256. Do not attribute that driver change to backend speed.

## Implemented changes and evidence

- Worker lifecycle responses read the durable registry activity revision without
  decoding all registrations. The idle parker reloads its candidate inventory
  only when that revision changes. A local 200-row microbenchmark reduced the
  revision read median from 37.86 ms to 0.53 ms; these are Mac measurements, not
  end-to-end production latency. Live stack samples confirmed the repeated full
  registration scans disappeared.
- Relay wake HTTP uses asynchronous I/O instead of a fixed 48-thread fleet-wide
  queue. Deadlines, stable operation identity, explicit overload retries,
  cancellation handling, response bounds and transport epochs remain enforced.
  This change alone moved queuing into gateway placement and did not improve
  end-to-end latency.
- Gateway local-wake admission reuses its initial capacity decision and refreshes
  only a blocked owner, outside placement locks. Exact-owner wake reservation and
  pending-demand removal commit in one SQLite transaction. The state-only update
  avoids rewriting spec/checkpoint payloads and avoids the unrelated Python
  inventory mutex; SQLite still provides the cross-process ownership fence.
- **Rejected experiment:** routing connections were reused exclusively between requests, with rollback
  of unfinished transactions, per-lease permissions checks, file identity checks,
  and disposal after SQLite errors. The idle cache bounds retained connections,
  not request admission. A local 32-wake/512-route profile fell from about 0.76 s
  to 0.48 s, but the replacement-worker live run did **not** establish a latency
  improvement. In the same-three-worker pair, wake p95 changed from 4.39 to
  5.24 seconds (median 2.85 to 2.54 seconds). This does not establish a loaded
  benefit, so connection reuse and its experimental tests were removed from the
  final patch. The raw reports remain.

- Local-wake shadow diagnostics reuse the fresh pre-reservation owner view
  acquired by admission. Diagnostics run after placement locks are released;
  they do not choose admission or reuse a stale TTL snapshot. A concurrent wake
  that bypasses capacity reservation falls back to a fresh observation. Portable
  migration planning retains its existing fleet view.
- Local routing writers share a small mutex around their existing transactions,
  avoiding competition in SQLite's busy handler. This preserves durable commits
  and cross-process fences, but does not add write parallelism or FIFO fairness.
  It is a mitigation, not evidence that SQLite's ultimate capacity has been
  reached. See the [database options assessment](../../reviews/gateway-database-scaling-2026-09-21.md).

The candidate methods were temporarily installed in running processes with
rollback timers, not packaged as a new release. Method-source hashes are in the
candidate reports. Release/version health responses therefore continue to show
0.5.74. No SDK or Verifiers changes were required for these backend experiments.

All temporary worker, gateway and relay patches were explicitly restored after
the final run. The comparison reservation was deleted, and the final check found
no sandbox routes, pending demand or prepared reservations. Public gateway health
was successful in five probes (median 24 ms); relay health also passed. The
implementation is in the working tree, not committed or permanently deployed.

Final validation passed 1,063 local server tests (six skips), 248 focused tests
under production Linux/Python 3.14, Ruff and diff checks. Earlier canonical checks
also passed Go, package builds and 118 SDK tests. Shellcheck was unavailable and
explicitly skipped; shell syntax checks passed. No shell sources were changed.

## Remaining bottlenecks

Selected traces after the asynchronous relay change spent about 9.5 seconds
waiting for gateway placement while worker restore took 0.48–0.66 seconds.
Gateway stacks showed the placement lock waiting on unrelated inventory/program
writes and repeatedly decoding owner inventories. With the gateway candidate,
selected placement spans fell to 0.15–0.35 seconds. Selected spans explain
mechanisms; they are not population quantiles and must not be added to p95s.

The replacement-worker run exposed a different queue: selected requests waited
4.1–7.8 seconds for worker restore admission, followed by 1.9–3.2 seconds inside
restore. Mounting/validating storage, lifecycle journal commits, runsc commands
and process fencing all contributed. The 16-slot sample still had 58–68% worker
CPU idle, while disk writes were 151–361 MiB/s and one worker's disk await was
16.2 ms. Aggregate idle CPU does not prove additional concurrent restores are
beneficial: storage and serialized Python/control-plane work can remain limiting.
The gateway process used about one CPU core on its two-vCPU VM.

The next substantial work is reducing per-wake inventory/transaction work and
storage/lifecycle latency, then repeating the same loaded test. Neither removing
all admission controls nor merely increasing a restore count establishes the SLO.
Ownership checks, durable parking, memory accounting and correctness checks must
remain intact.
