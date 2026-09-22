# Mixed-load wake qualification, 22 September 2026

The acceptance target is p95 response-ready to usable execution below one second
under a 256-sandbox workload, with zero scenario, health, poll, or cleanup errors.
Cold provisioning must overlap model responses. Fast results after provisioning
cannot compensate for a slow provisioning phase. No release below has yet met
that target.

The Linux driver uses 512 MiB incompressible resident memory, 128 MiB dirtied per
cycle, filesystem changes, a child-process tool, and 20–25 second model waits.
Eight cycles per sandbox produce 2,048 observations; 24 concurrent fleet pollers
exercise control-plane contention. Natural parking is observed, not forced.
The current usable-execution probe also waits for a complete 512 MiB integrity
hash. Raw wake, guest verification, and tool costs are reported separately.

## Changes and evidence

- `2769b1d` / 0.5.85 retained model waits according to pressure and queued demand,
  replacing a fixed retention expiry. The rolling-start test exposed gateway
  priority inversion: scheduling joined the same scan queue as public polls.
  The diagnostic run was interrupted after approximately 1,352 cycles and
  cleaned up; it was not a completed qualification.
- `f8600c8` / 0.5.86 let scheduling bypass the public scan queue and made unchanged
  program projections read-only. All 2,048 cycles completed correctly. Post-warmup
  wake p95 was 15.007 s and response-ready-to-verified-execution p95 was 19.322 s.
  During-provisioning p95 was 19.416 s; after-provisioning p95 was 1.182 s.
  The run started with one ready worker and autoscaled to four, with an uneven
  final assignment (113/83/31/29). This is a failed performance qualification.
- `45f9dca` / 0.5.87 replaced the infinite demand sentinel for any queued create
  with deduplicated actual requested memory. Timeout/failure cleanup, active-lease
  deduplication, and continued drain behavior were covered by 48 Linux tests.
  All four workers were ready before this run; it is not a controlled comparison
  with the cold-fleet 0.5.86 run. Profiles still showed gateway scan and writer
  contention, package metadata reads under placement, and repeated image-warmup
  writes from heartbeat handlers. All 2,048 cycles completed with zero scenario
  errors, but wake p95 was 12.024 s and full verification/execution p95 15.277 s.
  This was also a failed performance qualification.
- `e228bce` / 0.5.88 coalesces concurrent public fleet scans and JSON encoding,
  caches the immutable process release version, moves an inode check outside the
  reader-pool mutex, and avoids repeated image-warmup writes. There is no fleet
  snapshot TTL: subsequent requests refresh. Failed scans propagate to all joined
  readers and do not poison the next read. SQLite durability and generation/
  image fences are unchanged. 166 Linux tests passed.

The 0.5.88 isolated comparison used 24 readers, 32 lifecycle writers, and two
inventory reconcilers with 128 routes containing 16 KiB metadata. The identical
workload fell from 3.230 s to 0.594 s; fleet-list p95 fell from 0.463 s to 0.055 s.
Writer p95 improved only from 0.156 s to 0.141 s. These component numbers do not
prove the production wake target. The benchmark records module origins to ensure
that the baseline wheel and candidate checkout are actually distinct imports.

Diagnostic production runs used short stack samplers. Release packaging also
ran on the gateway during portions of the diagnostic runs. They identify
bottlenecks but are not clean comparative SLO measurements. A final qualification
must run without profiling or package construction on the gateway.

All upgrades preserve the qualified AgentEnv v0.2.2 native storage binary and
non-product dependencies byte-for-byte. Worker agents are upgraded only after
our synthetic sandboxes and relay requests have been cleaned up; native storage
service PIDs are verified unchanged. The four workers upgraded to 0.5.87 were
12399471, 12399473, 12399474, and 12399475. Retired 12399470 was separately verified
in provider SUCCESS state, without manufacturing a new termination request.

Artifacts live under `docs/benchmarks/release-0.5.84-2026-09-22/` through
`release-0.5.88-2026-09-22/`. Retained diagnostic and failed results must remain
visible alongside subsequent improvements.

## Further inventory bottleneck

The 0.5.88 run completed 2,048 cycles correctly but again failed performance:
wake p95 13.028 s and response-ready through verification/execution p95 16.862 s.
It missed the SLO before profiling started. The subsequent writer sampler showed
48–70 queued mutations and inventory reconciliation rewriting whole routes.
The fleet expanded from four to six workers; increased worker count did not fix
gateway queueing.

`710ab01` / 0.5.89 keeps one writer transaction and all existing incarnation
checks, but reads inventory routes together, batches observation watermarks,
storage dependencies and pending-demand removal, and decodes only absent routes
for absence reconciliation. Actual lifecycle/snapshot changes retain the full
existing update path. 164 Linux tests passed, including a concurrent incarnation
replacement, stale heartbeat, dependency, and unchanged-route regression.

A separate isolated test ran on the idle production gateway using its Python
3.14.4 and four vCPUs. Lifecycle write p95 fell from 444 ms to 231 ms; heartbeat
p95 from 422 ms to 209 ms. A 1 ms Python GIL switching quantum did not improve
the candidate (writer p95 246 ms), so that runtime tweak was rejected. The test
used temporary databases and an isolated test-dependency directory, without
changing installed service dependencies or production data.

The 0.5.89 full run completed all 2,048 cycles correctly but failed again: wake
p95 12.451 s, verification/execution p95 15.897 s. Subsequent sampling found
59–78 queued routing writes, now dominated by program phase transitions and
state-only wake commits.

## Lifecycle write reduction (0.5.90 candidate)

Commits `2930a2a` and `6d065a0` combine warm response-readiness and dispatch
timestamps in one durable transition, join the program/generation read under
the writer fence, return the exact inserted projection without a redundant SQL
readback, and update lifecycle columns without rewriting immutable specifications
or generation high-water records. Cold placement retains distinct ready and
dispatch transitions. The route commit still precedes acknowledgement.

SQLite files are now created with mode 0600 before SQLite opens them. Sidecars
are audited when a connection opens, rather than repeatedly stat-ing all three
files inside every pooled read and transaction validation. Main-file identity
and permission checks remain on every access; writer validation still surrounds
commits. Linux tests under umask 000 verify initial WAL/SHM modes and recreation
after all connections close. 167 tests passed, plus the strengthened 13-test
routing-pool suite.

An attempted continuous 4 MiB JSON-encoding stressor was stopped after more than
three minutes without a completed sample. It was a deliberately extreme isolated
CPU stressor, not a production latency result; it provides no valid A/B outcome.
No GIL switching change was deployed.
