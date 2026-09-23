# Performance architecture: implemented changes and remaining gates

> Historical candidate ledger. See [current production status](current-production-status.md)
> for the deployed rc22 baseline and the separately deferred changes.

This is an ongoing implementation and qualification of the
[architecture plan](../performance-architecture-plan.md). Production is testing
**0.5.114rc12**, installed at 12:51:09 UTC. Its wheel SHA256 is
`58a40891d4d859bb8d9d42914c062173925e829c95ca22004b6cb074ee7bfad9`.
It includes the tested gateway enqueue ownership fix, bounded small-upload RPC
transport, background CPU sampling, startup heartbeat validation, additional wake
incarnation fences, qualified filesystem-sized XFS journal selection, and bounded
immutable-chunk retry. The complete frozen Linux/real-PostgreSQL suite passed
1,554 tests with 12 expected skips. P4 remains disabled in production.
Frozen candidates are
not interchangeable with the shared working tree. Final server publication and
loaded performance acceptance remain incomplete.

The coordinated SDK **0.4.26** is published from `e728daf`; Verifiers `d391717`
is pushed with that immutable wheel pin and no local SDK source override.
Frozen server candidates retain separate metadata and wheel hashes; existing
published wheels were not replaced. The [completion audit](performance-architecture-completion-audit-2026-09-23.md)
separates implemented behavior, release proof and conditional extensions.

## Implemented

| Area | Canonical path and behavior | Removed or consolidated |
| --- | --- | --- |
| Response acceptance | PostgreSQL response and delivery obligation commit before the worker receives its receipt. Wake proceeds independently. | HTTP acknowledgement readiness waiters and their duplicate polling state |
| Database facilities | `PostgresDatabase` supplies live relay pool/transactions and relay-only DDL/status. | Live code no longer constructs the unshipped scheduling store; experiments use explicitly named qualification commands/store |
| Lifecycle transport | `relay_lifecycle.py` owns the production async gateway lifecycle protocol. | Test-only synchronous implementation and duplicated CLI encoding/retry logic |
| Linux resource evidence | One low-frequency background collector exposes scoped cgroup, memory and eligible guest-device I/O evidence; request handlers read its cache. | Duplicated memory/PSI parsers; pressure scale-up now reports exhausted capacity budgets |
| Immutable environments | One strict environment identity and rootfs manager boundary; Docker fingerprints and on-disk writers remain unchanged. | Provisioner no longer accepts a second independently owned Docker store |
| SDK/Verifiers admission | One FIFO request budget per shared client, acquired before leasing work; sync and async paths match. | Per-session limits no longer stand in for experiment-level admission |
| Load measurement | Separate durable acceptance, guest-originated continuation and uploaded tool completion, with a versioned report. | Commit-as-wake aliases and external probes that could provide the initial wake |
| Sole live relay authority | PostgreSQL only at the 0.5.114 release boundary, with bounded offline SQLite import/fencing. | Live SQLite relay, notifier and retry engine |
| Memory/checkpoint architecture | RAM-active memory, owned XFS durable capture, native pre-reap workspace revision and one v3 component commit; remote sparse publication/import. | Coupled identity assumptions, private cross-component workspace access; legacy readers remain until drain |
| Wait and transition policy | Measured resident footprints, wait and park/wake history, bounded clean-cache reclaim, typed exact transition claims and shared memory forecasts. | Duplicate reservation state and speculative quiesced lifecycle states |
| Resource phases | Expiring sequenced advisory hints through the existing registration and park path; identical sync/async SDK contract. | No new lifecycle owner or parallel hint control endpoint on workers |

The request budget initially reserved a session's entire concurrency before
polling. Independent review reproduced 512 idle sessions using all 512 permits
while only 64 polls ran. The corrected implementation reserves one request per
poll and accounts for request capacity in poll rotation. A 512-session regression
proves 128 concurrent polls, progress for ready sessions at the tail, and full
permit recovery on cancellation. Control/commit/renewal connections stay separate.

The load observer uses a separate unbound relay rollout. The guest validates the
model response, executes its first tool and sends a nonce/cycle/digest receipt.
Only after receipt observation may the driver issue sandbox uploads or execs.
All receipts in a polled batch are timestamped before their acknowledgements.
The additional observation request/commit per turn is included in the measurement:
this is a conservative continuation bound, not pure runtime restore latency.
The benchmark gates continuation p95 below 0.8 s and uploaded useful-action p95
below 1 s, including provisioning phases. The production measurements below remain below the required qualification standard.

The PostgreSQL CI investigation reproduced a cancellation race on Python 3.10.13.
Explicit shutdown and cancellation-preserving waits fix it; the lane now selects
its interpreter explicitly and tests 3.10.13 and 3.13. See the retained
[PostgreSQL/socket evidence](../benchmarks/relay-acceptance-2026-09-23/README.md).

## Earlier integrated validation

These counts cover the first implementation checkpoint, not all later changes.

- Final integrated server suite on Linux/Python 3.13.2: **1,320 tests**, successful,
  **72 optional/infrastructure skips**. This includes the resource, rootfs,
  lifecycle transport and observer tests.
- Real PostgreSQL on Linux/Python 3.10.13: **62 tests passed** separately, including
  commit/acknowledgement/restart races and relay-only schema initialization.
- Combined PostgreSQL/relay/stateful tests on Linux/Python 3.13.2: **104 tests**,
  successful, one optional skip.
- Final SDK suite on Linux/Python 3.13.2, locked dependencies and all extras
  including Inspect: **131 tests passed**. The in-process 512-connection fixture
  needs descriptors for both socket ends; the first attempt hit the host's 1,024
  soft limit. The final run used an 8,192 process limit and passed.
- Verifiers local candidate: locked sync and **four plugin tests passed**.
- Both source distributions/wheels built; Python files inside the wheels matched
  the final sources. Both passed fresh Linux installed-wheel smoke checks.
- Repository Ruff and whitespace checks passed. New native storage/runtime
  behavior was not asserted from these tests.
- Isolated HTTP tests verified all 64 responses after relay-process loss and all
  512 responses in a concurrent burst. Their wake is a 600-ms simulation; they
  are not sandbox load tests. An owned PostgreSQL process-kill/restart check passed.

## Historical runtime and load qualification

- rc1 contains durable response acceptance, resident-wait policy and the async
  gateway event path. It retains the deployed native runtime and coupled storage.
- rc2 differs from rc1 only in bounded reusable SQLite connection leases. The
  frozen wheel passed 32 focused Linux tests; its native runtime and dependencies
  are byte-identical to the qualified 0.5.113 bundles. Deployed at 09:52 UTC after
  checking zero sandbox routes and no inflight/pending relay deliveries.
- These early candidates predate split memory/workspace, v3 publication/import,
  immutable artifact support and measured reclaim. RAM-active split backing is
  now enabled on fresh rc8 workers; the EROFS worker adapter remains opt-in.
- On an isolated kernel-7 Linux UCloud VM with real ublk/XFS, the split lifecycle
  passed enforced project-quota exhaustion, exact-original-runtime abort and
  three park/restore cycles (approximately 270–275 ms each). This is a single
  sandbox functional gate, not a density or latency claim at load.
- The split backing interference test exposes a real tradeoff: workspace flush
  latency fell from hundreds of milliseconds to roughly 1–3 ms, while physical
  guest-device writes increased for the zero-filled/page-sentinel fixture.
  The old compressed store benefits from compressibility. The new ordinary file
  still receives kernel dirty writeback while the sandbox is live. Raw ABBA
  evidence is retained in `../benchmarks/split-memory-2026-09-23/`; the RAM-active
  path now has an isolated 50%-entropy ABBA comparison: **87.8% fewer physical
  writes**, **14.3% more SQLite commits**, and commit p99 1.77–2.19 ms versus
  12.30–14.21 ms. All heap and filesystem integrity checks passed. These are
  running-workload results, not checkpoint or density qualification. Exact
  runtime/workload hashes and raw reports accompany `ram-active-summary.json`.

All load runs use the same pinned image, 512 MiB resident/128 MiB dirty memory,
1 GiB memory limit, 4 GiB workspace, 20–25 s model waits and uploaded tool checks.
Latency includes guest-originated receipt observation. None of these light
natural-wait runs proves dense actual park/restore behavior.

| Deployment / 512-agent run | Completed correct turns | Continuation p95 | Uploaded useful action p95 | Actual observed parks |
| --- | ---: | ---: | ---: | ---: |
| 0.5.113 baseline | 4,096 | 0.688 s | 1.580 s | 0 |
| rc1 first attempt | 246 before failure | incomplete | incomplete | 0 |
| rc1 repeat | 4,096 | 3.590 s | 7.863 s | 0 |
| rc3 directed SQLite handoff | 4,096 | 2.897 s | 6.367 s | 0 |
| rc4 batched idle poll reconciliation | 4,096 | 3.022 s | 7.105 s | 0 |

The first rc1 attempt failed a read-only managed-agent status request during
provisioning. A typed, queued management-read implementation and bounded
read-only driver retry were added. The repeat had no operation or cleanup
failures and one recorded relay poll retry, but substantially failed latency
qualification. It must not be described as a performance improvement. The rc2
run was stopped after a broadcast-wakeup bug produced hundreds of waiting
threads contending for the pool mutex. Blocking profiler sampling also perturbed
that run, so its latency is invalid for comparison. rc3 uses directed FIFO
handoff and completed all turns correctly, but remains slower than baseline.
rc4 additionally coalesces idle PostgreSQL poll reconciliation into one batched
readiness query; authoritative claim/authentication transactions still run on
initial poll, readiness hints and deadline. Its load trial completed correctly but still failed the latency gate; after
provisioning useful-action p95 was 4.281 s. Sparse nonblocking 5 Hz profiles were
used in two 20-second windows. Worker I/O pressure remained visible with low
CPU utilization; the combined RAM-active/static file helper candidate is next. Aggregate evidence is in
`../benchmarks/architecture-load-2026-09-23/`.

## Full RAM candidate

Frozen rc5 wheel SHA256: `756e29014fae10c0606a03bad30be637cefc550675629cb1deb6964aa278dec5`.
Its new native runtime is attested in the split-memory evidence directory; the
combined source passed 1,451 Linux tests (78 infrastructure/optional skips),
140 later focused regressions, and 85 installed-wheel bootstrap/runtime tests.
Python 3.10.13 real PostgreSQL/socket tests passed 123 cases (one optional skip).
The SDK passed all 132 tests on Linux with all extras and built successfully.
The runtime and file helper are replaced only on fresh workers; the Docker
rootfs adapter remains selected. Later wait-cost ranking and phase-hint work is
explicitly outside this snapshot.

## Assembled candidate regressions and current qualification

Native tests did not establish whole-worker boot correctness. rc5/rc6 accepted
no sandboxes: bundle attestation metadata, same-filesystem runtime layout and
the actual storage RPC client method each required an integration correction.
The real Unix-wire runtime/HTTP assembly fixture now covers both split backing
modes with a populated workspace registry. rc7 exposed a heartbeat workspace-ID
lookup error and a deferred-park lease receipt inconsistency. Both are corrected,
with exact workspace identity and actual SDK/PostgreSQL renewal regressions.
See [bootstrap evidence](ram-worker-bootstrap-qualification-2026-09-23.md).

| Candidate / trial | Application outcome | Continuation p95 | Useful action p95 | Qualification |
| --- | --- | ---: | ---: | --- |
| rc7 natural-64 | 256 cycles; zero parks | 0.276 s | 0.545 s | **Not qualified:** old harness incorrectly passed despite broken worker heartbeats. |
| rc7 forced-16 | Four cycles before failure | — | — | Failed strict SDK renewal: deferred park exposed an epoch without a matching monotonic receipt. |
| rc8 natural-64 | All 256 cycles; zero parks | 9.983 s | 10.943 s | Failed health and latency; kernel/SSH/I/O evidence identifies a system-level interruption without establishing its initiator. |
| rc8 natural-64 repeat | All 256 cycles; zero parks | 0.310 s | 0.786 s | Healthy passing warm trial; does not qualify forced restore or 256/512 density. |
| rc8 forced-16 | Interrupted after actual restore errors | — | — | Failed `_restore_cost` on real nested artifact metadata; rc9 correction installed, awaiting live rerun. |

The [rc8 interruption note](gateway-stall-2026-09-23.md) retains the matching
10.431-second kernel watchdog interval and 8.048 seconds of exported I/O waiting.
No provider, Python or database cause is asserted from that evidence. The failed
trial remains in the results. Report version 3 now requires fresh heartbeats and
resource evidence for this run's used workers when admin sampling is enabled;
fast recovered requests cannot hide a fleet-health failure.

The latest combined source gate passed **1,504 tests with 12 skips**. Subsequent
public workspace cleanup passed 134 focused Linux tests. The restore-cost repair
passed 12 transition tests including real sparse files, serialized v2/v3 manifests,
actual artifact-store publication/decoding and service-wake admission. These are
different snapshots, not one final deployed-wheel result. They complement rather
than replace the live forced-transition gate.

Native RAM qualification also passed full heap/TCP restoration, forced restore
OOM with retained-checkpoint retry, exact cgroup charging and physical reclamation.
The final runtime's entropy sweeps retained the physical-write reduction. A
1.5-GiB cold restore still takes roughly 1.35 seconds; resident retention is a
necessary part of the low-latency design, not proof of universal subsecond restore.

## Outstanding plan gates

| Plan slice | Current gate/work |
| --- | --- |
| P0 | Repeat controlled production measurements; fixed-fleet density and six-versus-eight-worker comparison; complete attributed resource costs |
| P1 | Implementation/retirement complete; final frozen release and CI qualification remain |
| P2 | Finish gateway lifecycle use-case extraction, then qualify measured whole-path behavior; no multi-host authority change |
| P3 | Rerun forced transitions after rc9 repair, qualify dense/sustained pressure and mixed reader/bootstrap behavior; native split/RAM proof and fresh-worker activation are complete |
| P4 | Core/API/GC/backend-loss and actual-kernel tests passed; heterogeneous-agent qualification and full image coverage precede EROFS worker activation |
| P5 | Actual fixed-fleet density/throughput remains unqualified. Typed cost accounting is implemented; no fabricated CPU/I/O caps substitute for measured control. Shadow footprint/startup-delay calibration needs prediction validation before enabling actions |
| P6 | Hints implemented. Advance capacity/artifact prewarming needs an actual useful phase consumer and measured benefit. Trainer supervisor remains contingent on a supported preemption contract |

## Release boundaries

Upgrade the gateway reader before workers emit additive resource evidence,
`memory_backing` capacity or the new resident-wait reasons; old strict readers
reject these fields/reasons. New readers accept older workers that omit them.
Keep the compatible gateway reader during any worker rollback. RAM-active workers
write split v3 checkpoints; retain compatible destination readers, exact runtime
provenance and rootfs ABI admission until those incarnations drain. The runtime
bundle must include the pinned executable/helper closure and its authenticated
build manifest; installing only a runsc executable is insufficient.

The worker direct-registry schema is now **5**. Opening a validated v3/v4 registry
upgrades it transactionally, preserving registrations and adding the durable
`managed_growth` table (and wake fences for v3). Start only the new worker process
against that database; an old binary cannot share or subsequently read the new
schema. There is no automatic schema downgrade. Prefer fresh workers for this
rollout. Rollback on a stateful worker must retain v5-compatible readers and
growth accounting; otherwise drain its incarnations with the compatible code
and replace the worker. Do not drop forecast rows, decrement `user_version`, or
restore a pre-upgrade database beneath live sandboxes: active and ambiguous
primary-process growth must survive restart. Disabling the split/RAM writer is
also a fresh-worker choice, not an in-place conversion of existing checkpoints.

The EROFS rootfs writer remains opt-in. At **13:39:41 UTC on 2026-09-23**, a
read-only effective production-config check found `immutable_environments`
absent, hence both worker and builder switches disabled; split and RAM memory
backing were enabled. Defaults also omit this configuration, both immutable
switches default false, and bootstrap passes artifact arguments only for the
explicitly enabled role. Docker remains the active adapter. Selecting EROFS or
rolling back to Docker requires a fresh worker; bootstrap refuses changing the
adapter beneath existing state. Component qualification is not permission to
enable the fleet automatically. Existing PostgreSQL scheduler experiment tables
are left untouched; only fresh live relay migrations stop creating them.

The coordinated SDK and Verifiers wheel-pin release step is complete. Verifiers
retains its existing local framework dependency, but no longer substitutes a local
SDK for the published 0.4.26 wheel. No runtime version-probing fallback was added.

Production density, real forced-restore tails and heterogeneous artifact performance
remain qualification work. The current evidence does not establish subsecond useful
action at 256 or 512 sandboxes. Historical failures remain visible even when a later
candidate fixes their triggering defect.

## rc9 integrated findings and rc10 gates

The fixed-fleet four-worker rc9 natural-256 run completed **2,048 turns**, with
healthy fleet telemetry and no observed parks. Continuation p95 was **0.488 s**;
useful-action p95 **0.932 s**. Provisioning-overlap p95 was **1.055 s**, so the
strict whole-run SLO failed despite a passing steady state. The full report hash
and distribution are retained in the architecture-load directory.

The forced-16 test exposed a distinct storage identity defect: the new capture
RPCs reused the caller's sandbox-local operation ID in a node-global storage
journal. One sandbox per node succeeded, while its peers hit conflicts and
retried. rc10 uses the existing canonical owner/generation/volume/step hash;
a real shared Unix storage service regression covers two owners, abort, replay,
and recovery of old prepared capture intents. The benchmark IDs remain unchanged.

An independent pressure test found that immediate memory-pressure rejection
removed restore demand before the resident-wait policy could observe it. rc10
keeps known startup/restore demand queued under the existing operation deadline,
with delete/drain cancellation and exact claim cleanup. It introduces no new
CPU or I/O concurrency ceiling.

The installed candidate now needs fresh-worker forced and larger-density trials.
No rc10 performance result is claimed by these component regressions.

## rc11 assembled load and next measured targets

Four fresh rc11 workers completed forced16 (64 turns, 48 measured actual parks)
with healthy telemetry and zero operation/cleanup errors. Continuation/useful-tool
p95 were **1.518/2.052 s**: correctness passed, latency did not. Sampled cold
restores took 0.362–0.832 s in runsc; gateway admission took 6–11 ms. A separate
foreground 50 ms CPU measurement is being replaced by a fresh interval from the
existing background sampler, preserving immediate memory/PSI checks.

Natural512 completed **4,096 correct turns**, healthy four-worker fleet, zero
parks and no operation/cleanup errors. Each worker retained approximately 128
sandboxes, using approximately 76–78 GiB with 12–14 GiB available. Worker I/O PSI
was generally below 1 during steady work. Continuation/useful-tool p95 were
**3.782/9.340 s**, so density/correctness improved but the latency gate failed.
The last cooldown portion had a 5 Hz nonblocking gateway profiler attached;
it returned too few samples to establish a dominant CPU function. This is not
an unprofiled acceptance pass.

A same-shape diagnostic repeat includes an explicitly nonblocking gateway
profile from approximately 12:37–12:38 UTC. It fell behind its requested 50 Hz,
was stopped, and returned 2,576 stacks with 333 read errors. Its sample counts
are not trustworthy exclusive CPU percentages. It identifies contention around
`AsyncNodeHttpPool` enqueue/completion guards and synchronous upload forwarding.
The next candidate moves wakeup syscalls outside the enqueue guard with owned
shutdown reservations, and buffers uploads no larger than one existing 64 KiB
transfer chunk for the existing pooled RPC transport. Larger uploads retain
streaming and truncated bodies still never dispatch a write.

Readonly worker CPU-cgroup sampling found roughly 1.5–2.9% throttled periods
at stable density and only 3.8–6.7 CPU equivalents used per 32-vCPU worker.
Quota stalls exist, but do not explain the fleet-wide multi-second tail. No
CPU quota is being removed on this evidence.

## rc12 fixed-fleet natural-512 result

The unprofiled `relay-load-f612461fc77c` run completed 4,096/4,096 correct
turns on four rc12 workers, with no workload or cleanup errors, 141/141 health
checks and 6,038/6,038 fleet checks passing. All waits remained resident; this
is density/retention evidence, not forced-restore evidence. Measured cycles
(excluding each sandbox’s first cycle) had continuation p95 **0.933 s** and
useful-action p95 **2.251 s**. Compared with rc11 natural-512’s 3.782/9.340 s,
this is a substantial combined improvement from foreground sampling, enqueue
lock and pooled small-upload fixes; the experiment does not attribute gains
individually. Strict latency qualification still fails. During provisioning,
continuation p95 was 1.358 s; after provisioning it was 0.867 s. Whole-worker
physical writes totalled 109.6 GB over observed intervals, including startup;
these counters are not per-sandbox attribution.

[Sanitized complete summary](../benchmarks/architecture-load-2026-09-23/architecture-candidate12-natural512.summary.json).

### rc12 cold and physical-pressure gates

The forced-64 run `relay-load-f5c44e30feda` completed 256/256 turns with 192
measured parks, healthy workers and no operation/cleanup failures. Measured
continuation p95 was **2.014 s**, useful action **2.492 s**: cold correctness
passed, latency failed.

The larger-working-set pressure-256 run `relay-load-398dd9b5be64` stopped after
66 completed first cycles because sandbox `0199` reported a terminal managed
agent with empty stderr. This run uses 1,536 MiB resident heap per guest (384 GiB
fleet-wide before overhead) on four workers with roughly 87.8 GiB usable RAM
each; it necessarily requires parking. Actual policy captures occurred, several
sandboxes entered recovery, and one worker heartbeat became stale during peak
pressure. All owned sandboxes were cleaned up, and all four nodes remained
alive. Bounded kernel journals showed no host/cgroup OOM record; the cause is
under investigation. This failed test does not qualify pressure safety or density.
The driver now includes managed-job state, exit code and signal in terminal
errors so an empty stderr cannot hide those facts in subsequent runs.

Pressure follow-up recovered durable managed-job records after cleanup: 16
guests terminated with SIGBUS (signal 7), including three on worker 12400421
at 13:04:49 UTC and thirteen on 12400419 at 13:05:01–03 UTC. Kernel journals
showed no OOM records on any of the four workers. SIGBUS establishes a guest
memory-mapping fault; it does not by itself establish tmpfs exhaustion. Admission
review independently found that the first transition bypassed the projected
MemAvailable-minus-2GiB check when there were no existing claims. That condition
is removed and covered with a real-service test where ample swap must not
authorize a physically oversized unswappable startup. RAM-backing capacity and
post-start guest growth remain separate investigations.

## rc13 assembled qualification and natural-512 result

The exact candidate passed 1,579 Linux tests against PostgreSQL and SDK0.4.26
(12 expected environment skips), plus 140 tests in the Python3.10 PostgreSQL/
socket lane. Wheel SHA256: `c0258698b4a6bc4b904a3e455acf8c9ef258aad94499db050663378772656a26`.
Installed 13:17:57 UTC; workers 12400470,12400471,12400473,12400474.

Natural-512 `relay-load-f8be0f00d190` completed 4,096 correct cycles, zero parks,
no operation or cleanup errors. Measured continuation p95 **0.951 s**, useful
action **2.452 s**: latency remains unqualified and is roughly unchanged/slightly
worse than rc12. Async small uploads and keyed PostgreSQL notifications have
component evidence but did not establish an end-to-end latency improvement here.
A separate diagnostic repeat is explicitly profiling worker registration and
lifecycle checks; it will not be used as an unprofiled acceptance run.

[Load summary](../benchmarks/architecture-load-2026-09-23/architecture-candidate13-natural512.summary.json)
and [slow-worker stage evidence](../benchmarks/worker-exec-2026-09-23/README.md).

## Agreed closeout scope

The user explicitly narrowed closeout to memory-growth/backing correctness,
a successful physical-pressure run, a sustained realistic workload, combined
review/documentation and committed/pushed/deployed release. Further latency
optimization is a separate task. The immutable-image path remains opt-in; its
broader production qualification is not a closeout gate. Existing latency
failures remain recorded and must not be described as an achieved subsecond SLO.

rc13 repository-256 `relay-load-284ad4392c73` passed 2,048/2,048 turns with
FULL-synchronous SQLite WAL and retained reader/writer state, no operation or
cleanup errors, and zero parks. Measured continuation p95 was 0.376 s, useful
action 0.878 s; its strict all-phase SLO passed. This qualifies the resident
repository workload, not pressure or cold restore.
