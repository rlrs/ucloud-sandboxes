# Architecture completion audit

> Historical candidate ledger. See [current production status](current-production-status.md)
> for the deployed rc22 baseline and the separately deferred changes.

**Current scope:** the user reopened the original performance objective after the
pressure measurements. Reclamation while retaining the live runtime and avoiding
eager whole-heap restoration are unfinished requirements. The narrower closeout
section below records earlier scope; it is no longer permission to stop at memory
correctness or to declare P5 complete. Final release is withheld.

This audit compares the accepted [plan](../performance-architecture-plan.md)
with the frozen rc18 source on 2026-09-23. Each load report records its actual
deployed version; source implementation and regression qualification are not
claims about deployed density or latency. The latest completed load evidence
includes rc12, rc13, the failed rc15–rc18 pressure trials and the rc18
long-budget pressure integrity pass with failed health/performance gates. The rc18 regression gate qualifies source correctness;
its physical-pressure and sustained-load checks remain separate.

## Implemented behavior

| Slice | Current source and qualification status |
| --- | --- |
| P0 | Shared background resource sampling, cumulative CPU and physical I/O accounting, phase-separated guest continuation/tool timing and placement-scoped fleet-health qualification are implemented. Stored heartbeat readers accept validated legacy nested metrics while preserving strict malformed-record rejection. A stale heartbeat cannot qualify as zero resource use. Controlled load qualification remains incomplete. |
| P1 | PostgreSQL is the sole live relay backend at the explicit 0.5.114 boundary. Live SQLite state/notifier/retry code is removed; the bounded idle importer and source fence remain. Real PostgreSQL HTTP/stateful/restart/import tests pass. Durable response acceptance is independent of delivery. See [retirement evidence](relay-postgres-retirement-2026-09-23.md). |
| P2 | Canonical async relay lifecycle transport, shared SDK request admission, separate control capacity, and the gateway event-response handoff are implemented. Transport-independent lifecycle commits now own worker proof, route CAS, program outcomes and snapshot compensation; local wake admission owns shared inventory, projected capacity and atomic reservations. The canonical wake-placement use case now owns blocked-owner refresh, publication, local-versus-remote admission and relocation outcomes, using the existing migration executor/journal. HTTP translates typed outcomes. Delete/recreate readbacks and completed-journal replays are fenced; do not describe every gateway route as asynchronous. See [boundary evidence](lifecycle-commit-boundary-2026-09-23.md). |
| P3 | Native pre-reap capture, quota-owned split components, one v3 completion, sparse remote publication/import, physical reclamation and RAM-active memory are implemented and activated on fresh production workers. Public Warden workspace read/preparation methods replace private cross-component storage calls. Runtime/native proof passed; assembled bootstrap and live forced-load tests have found additional defects, recorded below. |
| P4 | Signed environment manifests, reusable components, registry-backed verified demand loading, host EROFS/OverlayFS, canonical rootfs/API/GC and backend-loss fencing are implemented. Real Linux and production-kernel component gates passed. General native gVisor EROFS is unsuitable for required xattrs; the constrained native profile remains optional. Four heterogeneous Python/Node slim/tools images passed cold/warm Docker versus EROFS qualification with empty adapter caches on the production kernel; host page caches were retained. Single-component EROFS uses a read-only bind rather than an invalid single-lower overlay. This does not establish loaded fleet performance; Docker remains the activated compatibility adapter. See [raw qualification](../benchmarks/immutable-environment-2026-09-23/heterogeneous-images.json). |
| P5 | One measured resident-wait policy, deficit-only clean-cache reclaim, wait/park/wake observations, advice-based ranking, typed exact transition leases, memory forecasts and foreground/maintenance progress are implemented. Cold offload uses existing published checkpoints and fenced detach. RAM-backing headroom and durable managed-primary growth exposure now feed the existing admission/retention machinery. Pressure correctness and density/throughput proof remain distinct gates. |
| P6 | Authenticated, sequenced, expiring advisory resource phases are implemented through PostgreSQL, the existing park transport, sync/async SDK and Verifiers. They grant no lifecycle authority. Trainer-independent continuity is conditional on an actual supported preemption contract. |

SDK **0.4.26** is released from commit `e728daf`. Verifiers commit `d391717` is
pushed, pins that immutable wheel, and removes its local SDK source override.
This closes the previously outstanding coordinated SDK dependency release step.
The separate local Verifiers development dependency is unrelated to SDK pinning.

## Agreed closeout scope and remaining checks

The user narrowed release closeout to memory-growth/backing correctness, a
successful physical-pressure run, a sustained realistic workload, combined
review/documentation and commit/push/deployment. Further latency optimization is
separate. P4 remains opt-in; broader image rollout is not a closeout prerequisite.
The original subsecond and wider density goals remain unqualified where noted;
this scope change does not turn their failed measurements into passes.

The current `pressure_integrity256` profile retains 256 sandboxes, eight cycles,
1.5 GiB resident heaps, 384 MiB dirty working sets, 2 GiB sandbox bounds, natural
parking, 16 concurrent launch slots and every integrity check. Both per-sandbox
SDK requests and guest-continuation observation now allow **1,800 seconds**;
the workload still has its existing **1,800-second overall deadline**. Cleanup
runs afterward with its own bounded operations. These longer budgets separate
memory-crash/state-integrity qualification from the already failed three-minute
response requirement; they do not establish a latency SLO.

The harness now lets each primary exit after its final verified result and
requires **256 successful terminal outcomes**. Previously, completed primaries
slept forever while retaining their heaps, an unreclaimable end-state exceeding
fleet RAM. Correcting that end-state does not explain the earlier slow wakes and
does not turn earlier failures into passes. Server source remains unchanged from
rc18. Run [`candidate18-integrity-pressure256`](../benchmarks/architecture-load-2026-09-23/architecture-candidate18-integrity-pressure256.summary.json)
(`relay-load-e46611368211`) completed **2,048/2,048 cycles and 256 normal primary
exits**, with no workload errors, cleanup errors or recorded SIGBUS. Its
crash/state-integrity gate passed. **Overall health and performance did not pass**:
11 fleet-health failures comprised eight node entries from two failed resource
probes and three stale-heartbeat observations. Measured continuation/useful-action
p95 were **53.398/53.942 seconds**; maximum continuation was **173.173 seconds**.
The report is `correct=true`, `slo_passed=false`. This successful longer-budget
run does not relabel the original 180-second profiles or erase their failures.

| Retained rc18 pressure trial | Completed cycles | Failure and qualification limit |
| --- | ---: | --- |
| [Original budget](../benchmarks/architecture-load-2026-09-23/architecture-candidate18-pressure256.summary.json) | 502 | Managed launch exhausted its 180-second SDK budget; 28 fleet-health failures; cleanup passed. |
| [Extended launch budget](../benchmarks/architecture-load-2026-09-23/architecture-candidate18-pressure-correctness256.summary.json) | 1,282 | Sandbox 0021, cycle 3 exceeded the 180-second continuation deadline; wake succeeded at 180.308 seconds. Fleet health and cleanup passed; no SIGBUS recorded. |
| [Correct finalization, original continuation budget](../benchmarks/architecture-load-2026-09-23/architecture-candidate18-finished-pressure256.summary.json) | 713 | Sandbox 0029 timed out awaiting cycle 0 continuation; two fleet-health failures; cleanup passed. |

All three earlier trials failed their own acceptance criteria. The later integrity
pass is narrower than overall pressure qualification; no three-minute continuation
guarantee or health/performance pass is claimed.

1. **Complete pressure qualification beyond the crash/integrity gate.** rc12 natural-512 completed
   **4,096/4,096 turns** on four workers, with no workload or cleanup errors and
   all health/fleet checks passing. Excluding each sandbox's first cycle,
   continuation p95 was **0.933 s** and useful-action p95 **2.251 s**. Continuation
   p95 was 1.358 s during provisioning and 0.867 s afterward. All waits remained
   resident, so this run does not qualify cold restore. This is a substantial
   combined improvement over rc11 natural-512, but individual changes cannot be
   credited separately. The strict useful-action target still fails. See the
   [rc12 natural report](../benchmarks/architecture-load-2026-09-23/architecture-candidate12-natural512.summary.json).
   rc12 forced-64 completed **256/256 turns with 192 measured parks**, without
   operation or cleanup failures; continuation/useful-action p95 were
   **2.014/2.492 s**. Cold correctness passed and latency failed.
   rc12 pressure-256 completed **66 first cycles**, then failed with **16 guest
   SIGBUS exits**. All owned sandboxes were cleaned up and all four workers
   remained alive. Bounded kernel evidence found no host/cgroup OOM records;
   the SIGBUS cause is not established. The independent first-transition
   physical-headroom admission defect is fixed in rc13. RAM-backing capacity and
   durable post-start growth accounting are implemented in rc15, but have not yet
   proved pressure safety or explained those exits. The [implementation ledger](performance-architecture-implementation-2026-09-23.md)
   retains the earlier rc9/rc11 results and the failed pressure trial.
   rc15 pressure-256 then stopped after **61 completed cycles** when primary
   startup hit its growth-admission deadline. The benchmark's broad `create`
   label included `start_agent`; the sandbox had already been created and its
   agent uploaded. The pre-dispatch error lacked retry metadata. rc16 corrects
   that narrow boundary using the existing SDK retry contract; this failed run
   still does not qualify pressure correctness.
   The rc16 pressure retry completed only **3 cycles** before a temporary
   managed-status read timeout; its HTTP handler caught the semantic parent
   exception first and returned 409. It also recorded **5 fleet-health failures**
   (four failed resource probes and one stale heartbeat), with no cleanup errors.
   No SIGBUS was recorded, which is not proof of pressure safety. The
   status/log subtype catch is corrected for rc17 and exercised through actual
   HTTP with both released SDK clients. The rc17 retry completed **zero cycles**:
   worker polling exhausted PostgreSQL connection admission and returned 500.
   Four stale-heartbeat failures and an observer-cleanup error were recorded.
   Independent worker samples retained at least 20.9 GiB physical and 22.7 GiB
   RAM-backing headroom; 140 observed managed processes were running with no
   terminal failures in that snapshot. This does not establish pressure safety.
   rc18 classifies only pre-transaction worker poll/respond pool admission as
   retryable 503, preserving deduplicated completion and ambiguous model-request
   semantics. It also prevents missing or pre-wait memory samples from admitting
   many captures as one-byte claims: an unknown capture probes alone, while
   measured captures retain the existing byte budget. The failed pressure gate
   stays open. See the [rc17 report](../benchmarks/architecture-load-2026-09-23/architecture-candidate17-pressure256.summary.json)
   and [worker evidence](../benchmarks/architecture-load-2026-09-23/rc17-pressure-memory/summary.json).
2. **Run sustained realistic work on the release candidate.** rc13 repository-256
   completed **2,048/2,048 turns** with FULL-synchronous SQLite WAL, retained
   reader/writer state, no operation/cleanup errors and zero parks. Continuation
   and useful-action p95 were **0.376/0.878 s**, and its strict all-phase SLO passed.
   This establishes resident repository correctness, not cold restore or
   pressure safety. A sustained candidate run with integrity and cleanup checks
   remains required. `candidate18-sustained512` is now running with **512 sandboxes
   × 32 cycles and standard request/continuation budgets**; it has no result yet.
   The wider original three-run 64/256/512 matrix,
   heterogeneous fleet images and density/bytes-per-turn comparisons remain
   follow-up qualification rather than being silently marked complete.
3. **Complete release bookkeeping and deployment.** Preserve reader-before-writer
   ordering for new heartbeat capacity fields/reasons, worker registry migration
   compatibility, rollback limits and retained v2/Docker readers. Finish combined
   review, record pressure/sustained results, and commit/push/deploy the qualified
   package. P4 stays opt-in. Provider bootstrap capabilities must be validated;
   UCloud tmpfs/kernel qualification is not evidence for every Hetzner host.

rc13 natural-512 separately completed **4,096 correct cycles**, no errors and zero
parks; continuation/useful-action p95 were **0.951/2.452 s**. This remained outside
the latency target. Its added transport/database changes have component evidence,
not a demonstrated end-to-end latency improvement. See the
[implementation ledger](performance-architecture-implementation-2026-09-23.md).

The exact rc10 package gate passed **1,529 tests with 12 expected skips** against
real PostgreSQL and SDK 0.4.26. That snapshot deliberately retained the previous
P4 adapter modules/tests while the single-component correction was qualified.
The exact rc11 full gate includes that correction and the nested heartbeat reader
fix: **1,531 tests passed in 189.357 s, with 12 expected skips**. Evidence is in
[the regression ledger](../benchmarks/architecture-tests-2026-09-23/README.md).
The exact rc12 package passed **1,554 tests in 190.923 s, with 12 expected skips**;
the combined Python 3.10 PostgreSQL/socket lane passed **133 tests in 21.398 s**.
The subsequent P2 domain/gateway/routing gate passed **219 Linux tests in 56.847 s**.
The exact rc13 package code then passed **1,579 Linux tests in 197.099 s**, with
12 expected skips, real PostgreSQL and SDK 0.4.26, without source exclusions.
This assembled gate includes the completed P2 use case and stale-publication
races, async small-upload handoff, keyed PostgreSQL delivery and the first-claim
physical-headroom fix. Packaging changes only the frozen version metadata from
0.5.113 to 0.5.114rc13; tested package code is unchanged.
The same mirror passed the combined Python 3.10 PostgreSQL/socket lane:
**140 tests in 26.984 s**, without timeout or thread hang.
The exact rc15 source gate passed **1,613 Linux tests in 200.041 s**, with 12
expected skips, real PostgreSQL and SDK 0.4.26, without exclusions. It includes
required exec admission, backing capacity, durable growth and the two review
fixes for unknown host metrics and imported-primary terminal observations.
The intervening rc14 gate's eight shared-fixture errors remain in the ledger;
replacing that stub with the actual registry resolved them.
The unchanged rc15 mirror passed **165 tests in 27.493 s** in the combined Python
3.10 PostgreSQL/socket, exec, growth and backing-policy lane, without a hang.
The exact rc16 package then passed **1,621 Linux tests in 205.759 s**, with 12
expected skips, real PostgreSQL and SDK 0.4.26. Its changed admission paths also
passed **48 Python 3.10 tests in 10.875 s**, including actual HTTP safe retries
through the released sync/async SDK. The pressure and sustained-load gates are
not inferred from these regression results.
The exact rc17 package passed **1,625 Linux tests in 211.037 s**, with 12 expected
skips, real PostgreSQL and SDK 0.4.26, without exclusions. Its corrected read
handlers passed **10 Python 3.10 tests in 6.460 s**, including actual HTTP
status/log retries through both released SDK clients.
The exact rc18 package passed **1,635 Linux tests in 212.733 s**, with 12 expected
skips, real PostgreSQL and SDK 0.4.26, without exclusions. It includes the precise
worker pool-admission contract, post-wait memory freshness and unknown-capture
fanout regressions. The unchanged mirror also passed **117 Python 3.10 tests in
17.275 s** covering the PostgreSQL and resident-policy paths. This is source
qualification, not a pressure-load pass.
Counts from different snapshots are not added together or presented as a deployed
performance qualification.

## Measured extensions, not invented controls

The transition ledger now records exact claims and known/unknown costs. Startup
uses a declared memory bound; restore uses authenticated allocated artifact bytes;
capture/publication buffer costs remain explicitly unknown. CPU/device/I/O byte
fields do not constitute qualified independent bandwidth budgets. Add a measured
controller only if mixed-load evidence identifies contention and establishes its
resource model. Do not create arbitrary CPU/I/O ceilings merely to match a plan
table. Existing physical storage/device guarantees and operator resource limits
remain explicit.

Program-demand calibration now combines historical cgroup charge, exact
incarnation/freshness fences, observed residual waits and measured provider-ready
delay in **shadow**. Unknown evidence retains full declared demand. It does not
change the existing action formula. Consolidation cannot use a smaller historical
footprint to weaken full-shape destination guarantees. Prediction-based activation
requires comparing forecasts against later demand, rather than treating a new
fixed weight as measured capacity.

P6 prepared-capacity or artifact-prefetch consumption is conditional on an actual
caller providing useful advance phase information and demonstrated startup
benefit. Current hints improve wait ranking; no independent prewarming scheduler
has been added. Likewise, cold offload intentionally selects already published
parks. Uploading an unpublished park to recover disk is an extension for an
observed disk-headroom case, charged to the same existing transition machinery;
publishing every wait would undo the write-volume reduction.

## Genuine deferrals and rejected alternatives

- Extra quiesced states were rejected after native resident-versus-paused reclaim
  ABBA found no reliable gain and added thaw latency. RUNNING retention, measured
  eligible clean-cache reclaim and durable park form the canonical policy.
- Trainer-independent rollout continuity remains a separate supported-contract
  milestone. Existing session scope does not promise trainer restart continuity
  or exactly-once tool effects.
- Smaller memory/disk guarantees and allocator chunks require worst-case proof;
  sparse typical usage cannot reduce a hard reservation.
- Docker/v2 readers are compatibility obligations until their incarnations drain.
  General native EROFS, another runtime, Redis, distributed filesystems and
  horizontal gateway mutation are not prerequisites for this release.

The physical-write improvement of RAM-active memory is real isolated evidence,
not yet a claim of subsecond loaded wake at 256 or 512. The historical failed
trials and remaining gates are retained in the
[implementation ledger](performance-architecture-implementation-2026-09-23.md).
