# Performance architecture and simplification plan

Implementation progress and qualification limits are recorded in the
[2026-09-23 implementation ledger](reviews/performance-architecture-implementation-2026-09-23.md).

Status: proposed implementation plan, 2026-09-23. No runtime changes or production
experiments are authorized by this document alone. Reviewed against server
`0144f5cee96346ae46e109ecb7336c8e4a785485` (0.5.113), SDK 0.4.25, and the available
Verifiers checkout `61a313a` (which still pins SDK 0.4.23). Recheck the actual
runner revision before integration qualification.

This is the next execution plan linked from the [simplification ledger](simplification-roadmap.md).
The supplied architecture assessment is directionally sound, but its server
baseline (`4175036`) predates important changes. This plan extends the deployed
mechanisms and assigns deletion criteria to their replacements. Historical
benchmark results are evidence of mechanisms, not forecasts of improvement.

## Decisions

1. Keep the managed-agent relay contract, gVisor, Warden runtime ownership,
   storage-native writable filesystems, generation fencing, and paused restore
   candidates. Do not introduce another runtime or distributed filesystem.
2. Finish the existing durable relay path: acknowledge retained responses
   independently of wake, using the existing PostgreSQL dispatch machinery.
3. Separate live memory backing from workspace durability. One complete
   checkpoint manifest binds both; component stores never become execution
   authorities.
4. Make resident, quiesced/reclaimed, and durable hibernated waits explicit
   choices of one policy and one Warden state machine. The middle tier ships
   only if runtime qualification demonstrates a useful benefit.
5. Define one immutable environment identity and rootfs lifecycle, with Docker
   and qualified EROFS implementations behind it. Avoid separate catalogs,
   publication protocols, or caching systems for each format.
6. Schedule hard storage guarantees, active working sets, and transition cost
   separately. Queue temporary overload fairly; keep physical safety and
   operator spending limits explicit. Removing all bounds is not the goal.
7. Pair each performance change with removal of the obsolete policy, state
   representation, or execution path. Moving functions into more files alone
   does not count as simplification.

## What is true in the current tree

| Assessment topic | Current evidence | Implementation consequence |
| --- | --- | --- |
| Durable response outbox | PostgreSQL already commits response and wake intent atomically, leases restart-recoverable work, and pins pending delivery (`shared_control/relay.py`). HTTP completion still waits for delivery in `model_relay.py`. | Change the acknowledgement boundary; do not build another outbox. |
| Queued park cancellation | Existing relay dispatch, CLI callbacks, and worker `relay_wake_fence` checks cancel delayed parks. | Consolidate and extend race coverage; preserve per-request fences when coalescing. |
| Resident model waits | `warm_park.py` already retains warm runtimes according to headroom and observed waits, potentially indefinitely. | Replace its policy in place; do not add another warm-retention controller. |
| Memory/workspace coupling | `DirectSandbox.memory_directory` identifies the workspace storage volume; Warden validates equal paths. Rootfs preparation also owns the memory annotation. | Separate references, layout, quota, capture, import, and cleanup in one vertical change. |
| Immutable images | Docker overlay2 shares already-materialized layers; storage-native lazy restore exists separately. | New immutable distribution must not duplicate mutable snapshot restore. |
| PostgreSQL scope | Live relay state uses PostgreSQL. Gateway routes/admission still use `RoutingStore` and its writer process. The generic PostgreSQL wake dispatcher is qualification code, not deployed ownership authority. | Keep one gateway authority. Isolate the unused prototype instead of accidentally activating a second scheduler. |
| Scaling and measurement | At 512 agents, 43/43 cycles in 05:58–06:02 UTC requested pressure scale-up, but six ready workers and `max_nodes=6` blocked it. Worker CPU medians were 10–23%, I/O full-stall medians 12–15%; gateway CPU was about 92% busy. | Test capacity and gateway changes independently; neither CPU nor PSI alone establishes efficient I/O. |

The [0.5.112 evidence](reviews/release-0.5.112-2026-09-23.md) has 256-agent
response-ready-to-tool p95 of 0.866 s and 512-agent p95 of 9.525 s. Both rolling
tests recorded zero actual parks. Forced restore was tested at 64 agents across
five workers, with tool p95 1.265 s. These are not dense 256/512 restore results.
The new sanitized [scaling evidence](benchmarks/cold-density-2026-09-23/scaling-summary.json)
retains the cap observation and worker summaries.

## Canonical boundaries and authority

Names below describe responsibilities; they are not a request for a framework
or one class per noun. Prefer small typed values and concrete modules. Introduce
a protocol only at an actual alternate implementation or process boundary.

| Boundary | Owns | Must not own |
| --- | --- | --- |
| Relay service and PostgreSQL relay store | Registration, inference lease, committed response, delivery obligation, retry schedule | Sandbox runtime or owner assignment |
| Gateway lifecycle service and routing store | Incarnation/owner, admission intent, migration journal, route transition | Process/device operations or a second response body store |
| Worker Warden and lifecycle journal | Safe point, quiesce/capture/thaw/restore, exact execution authority | Provider VM policy or independently interpreted scheduling state |
| Workspace store | Writable allocation, mounted lease, immutable revision, publication and release | Memory placement or runtime execution authority |
| Memory backing store | Quota-owned local files, capture and restore leases, durable component data | Another sandbox lifecycle state machine |
| Checkpoint manifest | One immutable binding of component generations, managed ledger, and runtime/environment identity | Mutable progress or two independent commit points |
| Environment manifest and rootfs manager | Ordered immutable components, composition semantics, rootfs lease and fingerprint | Checkpoint ownership or memory-directory policy |
| Resource policy and transition admission | Typed resource costs, priorities, fair queue decisions, observable capacity limits | Alternative quota ledger or provider-specific policy |
| Compute provider adapter | VM create/observe/stop semantics and provider loss proof | Relay, rootfs, or sandbox scheduling special cases |

Keep the separate journals where they correspond to real authority or privilege
boundaries. One canonical way does not mean one database for everything.
PostgreSQL is the target sole live relay backend; worker-local lifecycle and
storage journals remain recovery authority, and gateway SQLite remains the sole routed ownership authority
for this plan. Horizontal gateway mutation requires a separately qualified,
fenced authority cutover; adding HTTP processes does not authorize that change.

Extract transport-independent lifecycle use cases from the 9,071-line
`control_plane.py`, beginning with wake/park and result readiness. HTTP handlers
validate/authenticate/translate and call those use cases. Do not duplicate their
logic in an async frontend. SDK sync/async clients continue sharing wire contracts
and decoding, with only their I/O adapters differing.

## Delivery sequence

### P0 — Honest baseline, resource accounting, and contract inventory

**Change:** Extend `runtime_metrics.py`, `models.py`, `metrics.py`, and the
existing `live_relay_load_benchmark.py`. Collect physical-device bytes, IOPS,
latency and queue depth; cgroup CPU/throttling, dirty/writeback, faults/refaults;
checkpoint/publication bytes; hard reservations; and per-stage queue time.
Track device identity/reset and do not add stacked ublk/device-mapper/physical
counts together. Keep high-cardinality traces out of metric labels and sampling
off request-critical paths.

Unify `/proc` sampling currently split between `runtime_metrics.py` and
`background_io.PressureSampler` behind one typed sample with units and freshness.
Policies retain distinct decisions but consume the same evidence. Distinguish
unknown data from zero; separate node-global shared cache from private working
sets. The current mapped-file estimate is a useful signal, not exact attribution.

Fix `policy.py`'s generic pressure branch to report the specific exhausted
create budget. Retain the operator cap and expose blocked demand/queue age.
Compare six and eight workers with the gateway fixed, then compare gateway
changes with the worker fleet fixed. Do not attribute a combined change to one
cause. Pin versions, resource shapes, workload, and cache conditions.

**Cleanup:** Use one benchmark report schema and phase vocabulary. Reuse the
existing load driver and state-preservation fixture; remove obsolete latency
aliases and duplicated samplers after their consumers migrate.

**Gate:** Repeated Linux baselines with natural and forced waits, explicit actual
park counts, provisioning versus steady-state separation, and trace correlation
from model-ready through useful tool completion. Repair the hanging dedicated
PostgreSQL CI job before treating that lane as a release gate.

### P1 — Durable acceptance independent of delivery

**Change:** In `model_relay.py`, return a durable-acceptance receipt once the
existing PostgreSQL response-and-delivery transaction commits. Specify one
receipt contract: request identity, accepted/committed result, and delivery
status. Acceptance promises retention, not that the sandbox has resumed.
Reuse the existing leased dispatcher in `shared_control/relay.py`; no new broker,
Redis, polling table, or fire-and-forget task is needed.

Keep identical submissions idempotent and reject changed payloads under the
same identity. Delivery work resolves the current fenced owner, waits for
admission, wakes or confirms execution, and releases the response. Per-request
delivery obligations remain durable even if concurrent wake attempts for one
sandbox incarnation are coalesced. Each relevant late-park fence must still be
recorded; coalescing must not discard sibling request identity.

Define delivery completion separately from socket write success. Preserve the
existing authenticated retry/retention contract for a caller whose connection
disappears. Definitive caller loss terminates delivery without re-invoking the
model. Enforce storage budgets before accepting new inference, never evict a
committed undelivered response to make room for it.

Update the nested SDK and Verifiers together. Audit every completion caller for
the assumption that successful submission means awake. Retain a clearly named
delivery wait only for callers that need it; inference slots release at durable
acceptance. The load driver currently uses completion timing as wake timing and
must instead observe actual response delivery and first tool execution.
Specifically, prove in-sandbox receipt and continuation of the committed model
response before external upload/exec probes. Those probes can themselves wake
the sandbox and would otherwise hide a broken delivery executor; observing a
running state alone is insufficient.

**Cleanup:** Make PostgreSQL the sole supported live relay backend after an
explicit migration window. Keep a bounded one-way SQLite import/fence tool until
remaining deployments migrate; remove the live SQLite notifier/retry engine.
Extract database pool/transaction facilities from the mixed
`PostgresControlStore`; move or delete the unshipped generic scheduling
dispatcher/schema fixtures. New relay installs must not create unused scheduling
tables. Existing tables are removed only after checking for users, not blindly.
Fix CLI/status/docs to describe relay authority precisely.

**Gate:** Crash after commit/before acknowledgement and before/after wake;
dispatcher death and lease expiry; lost acknowledgement; duplicate/mismatched
payload; deleted or migrated sandbox; several simultaneous requests; queued
park versus result arrival. Prove response acceptance completes while wake is
deliberately blocked, without duplicate inference or lost delivery. Use real
PostgreSQL integration tests and the actual SDK/Verifiers flow.

### P2 — One request path with inexpensive waiting

**Change:** Establish the gateway lifecycle service boundary before introducing
an async HTTP adapter for long-lived waits and buffered RPCs. Use the current
bounded aiohttp transport as the starting implementation. Streaming upload and
download remain explicit methods of the same transport contract, with bounded
buffers, ownership fencing, deadlines, cancellation and no ambiguous mutation
replay. Preserve ingress/body-bearing connection protections.

Share one transport budget for a Verifiers experiment across all its sessions,
with fairness and cancellation. The existing shared relay client within one
interception instance is useful, but per-tunnel `max_concurrency=8` is not a
global budget. If an experiment spans processes, its supervisor owns admission;
an in-process semaphore cannot enforce that scope.
Acquire inference capacity before leasing work, or explicitly renew queued
leases; otherwise a shared semaphore merely converts overload into lease expiry.
Keep response-commit and renewal capacity available while inference/poll slots
are occupied. Preserve SDK sync/async behavior parity.

Profile authentication, routing reads/writes, parsing, inventory, thread/loop
crossings and worker I/O separately on Linux. The earlier fully async proxy
component was promising, while an async-to-blocking-handler bridge was slower.
Therefore migrate a whole measured request path; do not ship a stack of wrappers
or infer a Python/GIL diagnosis from one busy core.

**Cleanup:** Retire the old handler for each migrated route; use one bootstrap
selection during rollout, never request-by-request fallback. Replace duplicated
proxy retry/deadline logic with one policy and one structured outcome distinguishing
not-dispatched, committed, rejected and uncertain. Keep nonreplayable streams
explicit rather than forcing them through a generic buffered interface.
Move live lifecycle protocol encoding/classification out of CLI and remove the
test-only synchronous `_post_gateway_sandbox_lifecycle()` after migrating its
useful behavior tests to the production path.

**Gate:** End-to-end useful-action latency and CPU per completed turn improve at
fixed capacity. Test partial bodies, ingress reconnects, streaming backpressure,
client disconnects and shutdown. No transaction or worker execution slot remains
held across unrelated retry backoff. Do not claim horizontal gateway support.

### P3 — Split memory and workspace, with one checkpoint commit

This and P4 can proceed as independent implementation workstreams after their
shared environment/checkpoint identity contract is agreed. They need not wait
for the entire gateway transport migration.

**First prove the capture barrier.** Today Warden publishes `COMPLETE`, deletes
the runtime, tears down rootfs, then seals/releases storage; deletion cleans up
the runsc filestore. The new design requires an immutable workspace revision
captured while the old runtime is still quiesced and fenced. Qualify the backend
snapshot/flush behavior and filestore exclusion or cleanup semantics before
implementing the split. If no such operation exists, implement that capability
first. Do not reap early to work around the storage API.

Introduce explicit workspace and memory references in `DirectSandbox` and
`DirectSandboxRegistration`. Move the application-memory annotation out of
`OverlayRootfsManager` into runtime spec construction. Independent memory files
need enforced ownership/quota, crash recovery and writeback control; another
per-sandbox ublk volume reproducing the current path is not the intended fix.
Storage stays local for ordinary same-node hibernation.

Manifest v3 binds workspace revision, memory generation, kernel/allocator state,
managed-process ledger and exact environment/runtime identity. The sequence is:

```text
managed sandbox-wide safe point + fenced quiescence
  -> prepare immutable workspace revision and memory/kernel capture
  -> validate and durably retain both components
  -> commit ONE complete manifest
  -> reap old runtime and settle parked ownership
```

Restore prepares both leases from that manifest, validates the same generation,
starts a paused candidate, and grants execution through the existing authority
handoff. Remote publication/import references both components under one manifest;
incomplete component uploads grant no portability. Reference-aware reconciliation
cleans abandoned preparations without deleting dependencies of uncertain work.

**Cleanup:** Replace path-equality assumptions in `direct_warden.py`,
`direct_provisioner.py`, `image_rootfs.py`, `direct_registry.py`, and
`storage_native_migration.py`. Imports, creates and restores use the same supported
preparation/validation contract; services stop calling Warden private storage
helpers. One reservation calculation feeds gateway, worker and backing allocator.

**Migration:** Strict v2 manifests cannot gain ad hoc optional fields. Ship v3
readers before writers, preserve old manifest bytes/digests, and use one bounded
v2-to-internal-model decoder. New layout applies to new incarnations initially;
never move live memory files in place. Parked conversion needs a fenced transaction
and retains the source until the new manifest/ownership commit. Rollback disables
new allocations but keeps new readers until v3 instances drain. Remove old writers,
layout branches and the decoder after its documented retention window.

**Gate:** A Linux interference test combining dirty application memory and SQLite
commits must reduce workspace durability interference without material physical
I/O regression at fixed hardware. Measure bytes per turn; the split alone need
not reduce mandatory checkpoint bytes. Crash-test every preparation/commit/reap boundary, mixed component
generations, ENOSPC at peak overlap, candidate failure, remote import and GC races.
Separating paths does not imply separate physical disks or zero flush contention.

### P4 — Immutable environment artifacts and qualified EROFS

**Change:** One versioned `EnvironmentManifest` resolves base, workspace and
ordered toolkit digests plus composition semantics. Its resolved identity and
backend ABI feed the existing runtime/rootfs compatibility fingerprint. Preserve
existing Docker checkpoint fingerprints; a decoder must not relabel their bytes.

Generalize the actual lifecycle, not just `DockerOverlay2RootfsStore`'s type:
current preparation, restore leases, metadata and boot identity assume Docker
image IDs and `ucloud-overlay2-rootfs-v1`. Introduce one rootfs manager contract
for resolve/lease/prepare/rebind/release with a Docker adapter first. Keep mutable
workspace and execution checkpoint types distinct from immutable environments.

Builders produce independently reusable components. Specify and test precedence,
whiteouts, opaque directories, hardlinks, xattrs, ownership, symlinks and copy-up;
directory bind mounts are not merged-layer semantics. Initially keep existing
image IDs/API as the resolver input rather than adding competing public creation
paths. Environment publication reads a fresh allowlisted filesystem view, never
sanitizes a raw execution snapshot by deleting credential files.

Use the existing registry/content-addressed publication and reference machinery
for immutable metadata and blobs. Reuse existing verified range/cache mechanisms
where their contracts fit; otherwise extend one backend boundary. Add bounded
miss coalescing, range verification, cancellation, retry/backpressure, shared cache
leases and metadata/startup prefetch. A sparse ordinary file with missing ranges
is not a demand-loading implementation.
A whole-image digest cannot authenticate a fetched range before the remaining
image is available. Early exposure needs a trusted builder-produced chunk index,
Merkle proofs, or an equivalent verified backend contract. Content identity also
does not establish producer trust; validate provenance before privileged mounting.
Share these lower-level primitives without adding optional environment methods
to `SnapshotPublisher`, whose contract is execution-volume export/compaction.

Qualify host-mounted EROFS first with local validated artifacts, then demand
loading; probe the actual kernel configuration and supported feature set.
Qualify native gVisor EROFS initially for a constrained read-only toolkit profile.
The pinned parser supports read-only shared mapping but rejects incompatible
features; it is not interchangeable with every Linux EROFS artifact. Disk-backed
writable upper, file APIs, checkpoint FD reassociation and composition semantics
must pass before rootfs promotion. Do not assume gofer removal while other mounts
still require it, or flatten every component combination to obtain a benchmark win.

**Cleanup:** Both adapters use the same manifest, catalog, dependency leases,
GC roots, runtime fingerprint and test contracts. Docker remains a supported
adapter only for a documented requirement; otherwise remove it after migration.
Do not retain two environment builders or separate format-specific schedulers.
Docker/Buildx can remain a build/import input after Docker worker materialization
is retired; those are separate responsibilities.

**Gate:** Heterogeneous cold images and warm cache at equal workload/density;
measure downloaded/read bytes, startup latency, per-sandbox memory and CPU. Test
corrupt/missing ranges, malicious or unsupported image formats, interrupted
publication, GC concurrent with create/park/migration, and all filesystem semantics.
Native EROFS remains optional if it fails these gates.

### P5 — Wait tiers and cost-aware capacity

**Change:** Replace `WarmParkPolicy` with one pure wait decision policy using
expected remaining wait, active/reclaimable footprint, dirty bytes, queue delay,
capture/restore/refault costs and current pressure. Hints are advisory. The Warden
alone applies transitions through its journal:

| Choice | Runtime authority | Durability and recovery |
| --- | --- | --- |
| Resident wait | Existing live generation | No checkpoint promise; response continues normally |
| Quiesced/reclaimed | Same live generation and mounts | Durable wait intent, reconciled runtime identity; not a portable checkpoint |
| Hibernated | Complete manifest, no live runtime | Existing fenced local restore/remote publication contract |

Require a sandbox-wide managed safe point, including parallel tools, multiple
model requests and full-lifetime exec leases. Quiet output is insufficient.
Quiesce/thaw must leave the trusted control path able to accept a wake. Reclaim
is best effort with a byte-rate budget, observable achieved reduction, refault
feedback and bounded cancellation points. Do not promise page-level protection
using only a cgroup-wide reclaim knob. Runtime-supported selection of cold/free
ranges needs separate proof; never discard arbitrary live mappings.

Consolidate transition admission in `direct_service.py` around typed resource
costs and priorities. Reuse fair-capacity primitives, with protected progress for
response/wake, foreground tools and bounded maintenance. CPU, device and I/O
resources remain separate budgets, not one global semaphore. Keep stable cgroups
and adjust controls. Bound restore concurrency; do not revive unrestricted Go
scheduler startup. No admission wait should hold the lifecycle lock required to
complete or cancel another operation.

Replace the scalar disk formula with one phase-aware reservation breakdown:
workspace, memory backing, private checkpoint allowance, capture/restore overlap
and fixed overhead. Every consumer uses its total/projection. Change allocator
granularity or the second memory allowance only after proving worst-case
simultaneous allocation on the pinned runtime. Sparse/compressed typical usage
does not reduce a hard guarantee.

Add disk-pressure cold offload via existing publication/detach journals, ranked
by expected benefit minus upload/restore cost. Release local claims only after
verified remote publication and committed detach; reacquire them before restore.
Consolidation follows the same costs and ownership protocol. Enabling phase-aware
autoscaling requires calibration in shadow mode and hysteresis, not a new fixed
weight presented as a measured working set.

**Cleanup:** Delete the old warm timer policy and repeated admission calculations.
Create, wake, offload and migration use one reservation model and typed outcomes.
Temporary pressure stays queued internally until an explicit deadline/cancellation;
terminal unschedulable shapes remain distinct from transient overload.

**Gate:** At fixed correctness and useful-action tail latency, improve completed
rollouts per worker-hour and memory byte-seconds without increased I/O per turn.
Exercise restart at quiesce/thaw boundaries, missing runtime, PID reuse, stale
wake, caller cancellation, memory pressure and background starvation. A lost
quiesced node has no recoverable checkpoint unless one was explicitly committed.

### P6 — Verifiers resource phases and rollout continuity

Add advisory wait/tool/rollout-complete/training-pause events through the existing
registration/generation contract. Use batch knowledge for prepared capacity and
artifact prewarming; imminent response permits bounded prefetch, not unfenced
early execution. Hints cannot bypass admission or cause irreversible actions.

For trainer restart continuity, introduce one trusted rollout supervisor owning
session registration, trace/progress, policy version and grading state independently
of learner lifetime. Give it a durable lease/fence so two trainers cannot both
drive a rollout. Move existing forwarding/session ownership into that service;
do not leave a second trainer-owned lifecycle behind. Persisted rollout state
does not make arbitrary tool side effects exactly-once: reconnect to existing
operation IDs and represent uncertain outcomes explicitly rather than replaying.

This is a separate integration milestone, not a prerequisite for P1. Gate it on
actual trainer-preemption requirements and test reattachment, expiry, cancellation
and cleanup without duplicate tools or model calls. Keep credentials and grading
material outside agent-visible artifact publication.

## Qualification and release rules

The performance objective remains p95 response-ready-to-actual-wake below 0.8 s
and response-ready-to-first-useful-action below 1 s at the declared supported load.
These are targets, not achieved claims. Define useful action as a checksummed
uploaded tool performing representative filesystem work; report full working-set
verification separately and include it in throughput/correctness accounting.

Run 64 as a correctness smoke, 256 as the first density gate, then 512. Use both
natural independent waits and synchronized forced transitions. Include lightweight,
512-MiB/128-MiB-dirty, larger SWE working sets, repository/SQLite-heavy, heterogeneous
cold-image, and long-lived-session profiles. Report which profiles qualify;
hardware or hard-disk-fit failures must not be hidden by shrinking the workload.
Retain actual transition counts and residency; warm tests cannot qualify restores.

For each gate use at least three comparable steady runs, preserve distributions
and failures, and include a sustained run long enough to expose compaction,
publication, retention/GC and memory accumulation. Fix the fleet shape, background
traffic and cache condition within each A/B. Report completions per worker-hour,
CPU-seconds and physical I/O bytes per turn, byte-seconds resident, queue age,
deadline failures, hard-reservation use and cold/warm phase latency. Resource
utilization is diagnostic; unnecessary writes at 100% disk busy are not a win.

Linux is the runtime qualification platform. Use the pinned gVisor/AgentENV builds,
real PostgreSQL, guest workloads and deployed SDK path. Preserve existing memory,
process, socket, SQLite and cleanup checks, plus explicit crash failpoints and
property tests for authority transitions. Run contract suites against every
supported backend; avoid mock-only performance acceptance or source-text tests.

Each release must name its schema/capability changes, activation gate, mixed-version
behavior, retirement condition and rollback limit. New format writers remain off
until all eligible readers support them. Never roll an old binary onto a new
journal; disabling a feature does not undo persistent format changes. Run provider
contract/bootstrap checks for UCloud and Hetzner; core behavior stays provider-neutral.

## Dependency and deletion checklist

| Slice | Depends on | Required subtraction before completion |
| --- | --- | --- |
| P0 measurements/contracts | Current tree | Duplicate pressure samplers and misleading timing aliases |
| P1 response acceptance | P0 phase definitions | Wake-waiting submission; live SQLite relay after cutover; unused scheduling prototype in production package |
| P2 gateway/transport | P0; lifecycle extraction | Migrated blocking handlers and duplicate retry logic |
| P3 split backing | Capture-barrier proof; common identity contract | Memory/workspace identity conflation and private cross-module storage calls; bounded old decoder after drain |
| P4 artifacts | Common identity contract | Docker-specific assumptions above adapters; parallel catalogs/cache ownership |
| P5 wait/admission/offload | P0, P1, P3 | Competing warm policy and quota/admission calculations |
| P6 rollout supervisor | P1/P2 protocol contracts | Trainer-owned duplicate session/forwarder lifecycle |

Begin with P0 and P1, while qualifying the P3 capture barrier and P4 runtime
capabilities independently. P2 addresses the measured gateway bottleneck. P3/P4
are the structural investments; P5 exploits them. Do not postpone all useful
changes until a whole-system rewrite is complete.

Every implementation PR records the canonical owner and APIs, what it deletes,
behavior tests, performance evidence and residual compatibility. No generic
workflow engine, universal storage abstraction, duplicate retry framework or
backend switch belongs in the design without a concrete consumer. Separate
domain states that encode real crash boundaries even when doing so costs lines.

Deferred unless new measurements justify them: replacing gVisor, Firecracker,
lower-isolation container pools, 3FS, Redis/brokers, eBPF networking replacement,
core scheduling in provider VMs, stateless warm interpreter pools, multi-host
gateway ownership and speculative placement algorithms.

## Source map and qualification references

- P3/P5 pressure-path follow-through: [hybrid memory and retained-runtime
  reclaim](reviews/hybrid-memory-pressure-2026-09-23.md). The memory/workspace
  split alone does not qualify park/wake performance at the physical RAM limit.
- Current [architecture](architecture.md), [distributed state protocol](distributed-state-protocol.md),
  [PostgreSQL relay contract](postgres-relay.md), [shared control design](shared-control-plane-design.md),
  and [test remediation ledger](testing-review-and-remediation.md).
- Relay: `model_relay.py` completion handler; `shared_control/relay.py` response
  commit/claim/dispatch; `node_runtime.py` wake fences; `cli.py` lifecycle callbacks.
- Storage: `direct_warden.py` park/restore and storage validation;
  `hibernation.py` manifest and reservation; `direct_registry.py` registration;
  `image_rootfs.py` preparation/rebind; `storage_native_migration.py` import.
- Scheduling: `resource_admission.py`, `policy.py`, `program_scheduler.py`,
  `warm_park.py`, `background_io.py`, and `direct_service.py` operation admission.
- Linux documents best-effort [cgroup reclaim](https://docs.kernel.org/admin-guide/cgroup-v2.html)
  and kernel-dependent [EROFS file-backed mounts](https://docs.kernel.org/filesystems/erofs.html).
  Neither establishes support on our deployed kernel without a probe.
- gVisor documents [EROFS rootfs/mounts](https://gvisor.dev/docs/user_guide/filesystem/).
  Check the pinned source and qualify writable/restore semantics, rather than
  copying the documentation's memory-backed upper example into production.
  The reviewed pin is `50e1502a95d36ad2faf2c7ef33b8bf21fe975293`:
  [parser and supported features](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/erofs/erofs.go)
  and [filesystem checkpoint resources](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/fsimpl/erofs/erofs.go).

This plan was reviewed in three delegated, read-only workstreams: relay/transport,
memory/lifecycle, and immutable artifacts. No deployment, load test, or runtime
implementation was performed while preparing it.
