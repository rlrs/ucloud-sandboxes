# Production roadmap

Status: **Phase 0 qualified for production canary; Phase 1 infrastructure deployed in shadow**

Last reviewed: 2026-08-03

This is the source of truth for work after the storage-native runtime rollout.
The detailed parking, direct-runtime, and storage-native plans remain design
and qualification records; their old "remaining" sections are not separate
backlogs.

## Completed foundation

- One privileged direct Warden owns every sandbox lifecycle on a node.
- Docker/containerd are image build, pull, and content-addressed layer cache
  infrastructure
  only.
- gVisor park/wake works through the qualified SDK and Verifiers relay flow.
- Parked state is a durable storage-native manifest with lazy remote reads.
- Cross-node migration, route handoff, relay replay, drain evacuation, and
  hard disk admission passed production qualification.
- Release 0.3.69 qualified a 1.212-second storage-native migration protocol.
  The observed 71.48-second enclosing event was dominated by destination VM
  provisioning.

Production traffic on 2026-08-02 exposed two important boundaries. First,
direct-node heartbeat construction tried to inspect storage state for durable
`planned` and `quota_ready` registrations before a runsc sandbox existed. One
incomplete cold create therefore hid the whole node, marked its routes stale,
and induced replacement scale-up. Release 0.3.75 keeps those registrations in
reserved-resource accounting without inspecting unavailable backend state.

A subsequent parkable production sandbox exposed another heartbeat boundary:
its long-lived attached harness correctly held the sandbox lifecycle fence, but
heartbeat inventory joined that same fence and eventually made the healthy
sandbox and its node appear `unknown`. Release 0.3.76 reads the atomically
replaced lifecycle journal without joining active operations and makes TTL
cleanup opportunistic during heartbeat collection. Pre-0.3.76 agents remain
protocol-compatible with routes they already own, but are excluded from new
placement and autoscaler capacity until they drain.

A later create burst exposed a third boundary. A direct create reserved
transient CPU/RAM before calling the provisioner, but that reservation was not
part of the heartbeat or drain activity epoch. Cold rootfs materialization also
precedes the durable node registry record. The autoscaler could therefore close
admission, observe an apparently empty inventory, stop the node after its idle
grace, and leave gateway create handlers waiting for the ten-minute proxy
timeout. Those handlers saturated the 32-request gateway limiter while neither
pending demand nor host CPU/memory pressure asked the autoscaler for capacity.

The production design for burst scale-up is deliberately bounded:

1. Admission and transient-create registration share one node lock. Every
   accepted create is included in heartbeat work count, reserved CPU/RAM, and
   the drain activity epoch before image or rootfs work begins.
2. Gateway-busy trace samples are an explicit live signal, but only amplify
   independently confirmed node-side pressure. The default permits one
   temporary node beyond hard demand; raw gateway occupancy cannot scale alone.
3. Image locality remains preferred while a cached node has pipeline headroom.
   Once it is saturated, placement may copy the image to a less-loaded node
   rather than queue every registry pull behind one host.
4. Recent create pressure uses the ordinary pressure cooldown. Surplus nodes
   then enter the existing drain workflow; parked storage-native sandboxes are
   migrated only when exact destination disk-fit proofs succeed.

This is latency headroom, not disk overcommit. Temporary nodes never weaken
per-route hard disk reservation or node quota admission.

The same production burst also exposed a deeper image-path error. Even after
per-digest locking removed global serialization, the Warden flattened every
Docker image through `docker export` and tar extraction. On UCloud VMs the
original cache was below virtiofs `/work`; moving it to local XFS reduced one
representative pandas materialization from 76.342 to 16.374 seconds, but still
rewrote a 1.7-GiB filesystem already present as content-addressed Docker
layers. That intermediate result is retained in
[`benchmarks/rootfs-local-xfs-production-2026-08-02.json`](benchmarks/rootfs-local-xfs-production-2026-08-02.json).

The replacement production path preserves the layers. `DockerOverlay2RootfsStore`
inspects the immutable image ID, pins it with a digest-specific local tag, and
mounts Docker's existing overlay2 layer chain read-only. The ordinary
per-sandbox quota-owned overlay remains above that shared image view, so gVisor
and storage-native migration do not change. A completed image view is
reconstructed after mount loss, and sandbox resume reconstructs and revalidates
the image mount before attaching the writable layer. Unsupported graph drivers,
paths outside Docker's data root, unsafe mount options, and identity changes
fail closed; there is no silent tar-export fallback.

On production node `12362737`, the same 1.7-GiB, 14-layer pandas image took
16.663 ms to inspect, 20.393 ms to lease, 3.462 ms to mount, and 3.261 ms to
attach the sandbox writable overlay. The 43.779-ms hot rootfs setup is 1,743.8x
faster than the retained 76.342-second virtiofs flattening trace and writes no
archive or extracted copy. A real copy-up write passed and all temporary
benchmark state was removed. Raw results are in
[`benchmarks/rootfs-overlay2-production-2026-08-02.json`](benchmarks/rootfs-overlay2-production-2026-08-02.json).

The burst also showed that the generic live-pressure branch and the more
specific create-pressure branch could both interpret the same samples. Once
the bounded create-pressure target had been reached, generic pressure could
still add one node per cycle, bypassing
`create_pressure_max_headroom_nodes`. Create pressure now exclusively owns the
headroom calculation whenever both signals are true.

The latest production create failures exposed a separate positive feedback
loop in durable demand. A failed image pull or registry-reference lease was
persisted in the sandbox pending table, so the autoscaler treated the
post-placement failure as proof that another node was required. Each successful
VM submission consumed the row, a client retry recreated it, and a registry or
image problem could grow the fleet despite ample hard capacity. Pending demand
is now classified by whether another schedulable node can resolve it. Capacity,
wake, and migration-placement failures retain their existing scale-up behavior;
image-pull and registry-lease failures remain durable for retry identity and
diagnostics but are excluded from desired resources. A successful scale-up
consumes only the capacity-demand snapshot, and the metrics and demand APIs
expose the excluded count and resources separately. Optional registry-layer
overlap metadata is also hydrated asynchronously: a cold metadata request no
longer adds the registry timeout to sandbox creation merely to improve node
scoring.

Production traces from the same burst showed a separate image throughput
failure: a node accepted as many as 32 distinct Docker pulls at once, while
each pull was configured for eight parallel layer downloads. Create concurrency
is now decoupled from cold-pull concurrency. Nodes bound distinct pulls,
preserve all warm-create slots, expose active/queued/max pull telemetry, and
split pull queue time from Docker transfer time. Docker uses three layer
downloads per pull, preventing a 32-create burst from becoming roughly 256
registry streams. The limit is deployment-configurable and deliberately does
not trigger autoscaling: a transient registry queue is not hard sandbox
capacity demand.

An isolated 32-vCPU DFM node then pulled cold sets of real production images.
Four images took 52.278 seconds serially, 28.450 seconds at concurrency two,
and 16.298 seconds at concurrency four. Eight images improved from 39.565
seconds at concurrency four to 26.380 seconds at eight. With 16 images,
increasing concurrency from eight to sixteen improved makespan only from
58.047 to 54.755 seconds while pushing most individual pulls to roughly 55
seconds. The selected default is therefore eight distinct pulls with three
layer downloads each. Raw results are in
[`benchmarks/docker-cold-pull-concurrency-dfm-2026-08-03.json`](benchmarks/docker-cold-pull-concurrency-dfm-2026-08-03.json).

A Distribution pull-through cache was tested on the same node as an optimistic
upper-bound for cross-node sharing. Filling an empty cache took 37.167 seconds
for the matched eight-image set, versus 26.380 seconds direct from Docker Hub.
After pruning Docker's own images but retaining a fully warm proxy, the run
took 24.581 seconds: only 6.8% faster than direct. Pull still has to copy,
decompress, verify, and register every layer in the node-local Docker store.
The small hit-path gain does not justify routing all node traffic through a
2-vCPU gateway and slower shared storage. A shared proxy is therefore not in
the production path; storage-native/lazy OCI materialization remains the
larger future opportunity.

The runtime bundle includes `virtiofs` and its dependency closure alongside the
XFS, overlay, networking, and ublk modules, which makes an ordinary
filesystem-preserving guest reboot mount-safe. Production subsequently proved
that UCloud's post-start `SUSPENDED` transition is not such a reboot: resuming
replaces or clears the ephemeral guest, and UCloud may report `RUNNING` while
neither its SSH proxy nor private-network service has a reachable backend. The
autoscaler now treats that transition as permanent node loss. It fences the
heartbeat immediately, terminates the old job without a drain handshake,
removes non-portable routes as `node_lost`, preserves only published portable
park snapshots, and carries the lost hard-disk shapes into replacement-capacity
demand. Ordered job history and the provider stop journal latch the loss even
if UCloud later changes the current state back to `RUNNING`.

The superseded export pipeline remains as incident evidence. Its deterministic
32-image benchmark is recorded in
[`benchmarks/rootfs-export-pipeline-2026-08-02.json`](benchmarks/rootfs-export-pipeline-2026-08-02.json).
With four export slots it observed four overlapping exports, a 3.71x makespan
improvement over the serialized lower bound, and a 16.1 ms makespan for 32
concurrent warm cache checks. The production canary is recorded in
[`benchmarks/rootfs-export-production-canary-2026-08-02.json`](benchmarks/rootfs-export-production-canary-2026-08-02.json).
It observed three distinct exports running concurrently with no export waiters,
10/10 cold creates completing in 20.4--27.1 seconds, and two subsequent warm
creates completing in 1.09--1.20 seconds. All 24 exec checks succeeded and the
run cleaned up every route and reservation. Its retained metrics also exposed
two policy errors fixed in 0.3.84: the builder warm signal demanded a completely
empty full-disk node, and three occupied export slots were counted as 75% queue
pressure despite zero waiters. The zero-flatten store removes this export
pipeline from the production create path; the metric field names remain for
wire compatibility during rollout.

Second, the observed PRIME/TMax traffic eventually created `parkable=true`
sandboxes but still ran each harness through a long-lived attached exec. Those
sandboxes are permanently active for scheduling purposes; they cannot exercise
parked density or emit useful model-wait program state. The arbitrary-harness
production integration is not complete until it uses the generation-fenced
primary-job contract in [`durable-sandbox-jobs.md`](durable-sandbox-jobs.md).
An image command or `runsc exec --detach` is not a substitute for that contract.
Attached harnesses still consume real CPU and memory, but their nominal limits
must not be multiplied into permanent node reservations. Dynamic admission now
uses the individual sandbox shape, concurrent create/wake reservations, and
live host CPU, RAM+swap, load, and PSI. Durable primary jobs remain necessary
to make model-wait parking observable and safe; they are not a prerequisite for
correct burst-sensitive admission. For the observed 4-vCPU, 8-GiB-memory,
16-GiB-writable shape, the
parked hard-storage reservation is 33,856 MiB, so one qualified node can own
at most 42 such parked sandboxes and 64 require at least two nodes even with no
resident CPU or memory working set.

A 2026-08-03 production follow-up found 44 storage-native volumes behind
durable node registrations stuck in `deleting`. Failed wakes on the previous
agent had mounted new COW devices before obsolete remount-device validation
failed; deletion then encountered the same failure. The node reported only
one live sandbox but retained 1,441,600 MiB of hard reservations. Nominal route
accounting overcredited disk, so the autoscaler incorrectly reported zero
deficit for queued work.

The permanent model keeps node ownership authoritative. Failed wakes discard
uncommitted COW state, deleting registrations are retried continuously without
a client retry or node restart, and deletion remains replayable after either
physical quota or logical-ledger cleanup has already committed. Storage-native
free disk is clamped to the daemon's hard capacity-minus-reservations metric in
both placement and autoscaling. Gateway absence alone never authorizes storage
deletion.

## Phase 0: durable primary jobs

The runtime init, gateway job state, checkpoint/migration ledger binding, sync
and async SDK `JobHandle`, release bundling, and Verifiers qualification path
are implemented as specified in
[`durable-sandbox-jobs.md`](durable-sandbox-jobs.md). On 2026-08-03 an isolated
DFM deployment passed real same-node park/wake and a clean zero-node cross-node
migration from UCloud job `12362748` to `12362749`. The migration preserved the
managed-process ledger identity, reattached the relay, and completed one model
call and trace turn; its post-readiness protocol took 1.009 seconds. A
production canary remains before enabling program-aware scheduler actions.

## Phase 1: live-metrics autoscaling

This is the next production phase.

Implementation status:

- **Implemented locally:** SQLite/WAL store, sparse first-heartbeat lifecycle
  markers, indexed pressure/lifecycle reads, live-pressure headroom action,
  provisioning-p95 idle grace, cooldown hysteresis, decision telemetry, and a
  decision-oriented operations dashboard.
- **Program-aware extension awaiting the production canary:** durable
  rollout/request phase observation and terminal cleanup, per-return and
  periodic shadow wake planning, and bounded leading demand (action off by
  default) as specified in
  [`program-aware-scheduling-plan.md`](program-aware-scheduling-plan.md).
- **Verified locally:** focused policy/metrics/config/CLI/lifecycle and
  dashboard contract tests; 857 service tests passed with one skipped and 87
  subtests, and all 79 SDK tests passed.
- **Not deployed:** production metrics migration, canary observation, threshold
  tuning, and broad rollout.

Static resource requests remain hard safety constraints. In particular, disk
is never overallocated and a running or waking sandbox must fit its requested
CPU and memory on one node. Live metrics do not weaken those rules; they decide
how much ready headroom to retain around them.

The deliberately small v1 feedback loop is:

1. Create for pending or prepared resource shapes exactly as before.
2. Also create one headroom node when recent node heartbeats show sustained
   CPU, memory, memory-PSI, or storage-operation pressure.
3. Never create repeatedly while a headroom node is already provisioning.
4. Suppress scale-down for a bounded cooldown after live pressure.
5. Set the effective idle grace to the greater of the configured grace and a
   multiplier of measured VM submit-to-first-heartbeat p95.
6. Drain and evacuate only after the ordinary disk-fit and ownership proofs
   pass.

This avoids a workload predictor, per-sandbox probability model, or general
constraint solver. The operator-facing tradeoff is expressed with a few
direct knobs:

- CPU and memory utilization targets;
- maximum full-memory PSI;
- storage queue utilization target;
- pressure window, minimum samples, and freshness;
- post-pressure scale-down cooldown;
- provisioning-latency lookback and idle-grace multiplier;
- existing minimum/maximum nodes and create/provisioning caps.

Lower utilization targets spend more node-hours for lower wake/tool latency.
Higher targets increase density. Changing these values requires a config
reload in v1; a live policy endpoint is deferred until operational use proves
that restart-based tuning is insufficient.

### Metrics substrate

The production default is an indexed SQLite/WAL event database,
`metrics.sqlite`, shared by the gateway and autoscaler on the control host.
It replaces dashboard polling over rotated JSONL. Explicit `.jsonl` paths
remain supported as a rollback/read-compatibility mode, but receive no new
features.

The database provides:

- constant-size append transactions;
- indexed recent reads by event kind and time window;
- bounded retention;
- the same `/v1/metrics` snapshot consumed by the live dashboard;
- the latest autoscaler inputs, decision, and effective idle grace;
- VM provisioning and sandbox scheduling latency distributions.

The dashboard and autoscaler must consume the same observations and expose the
exact policy inputs used for each decision. A decision that cannot explain
which hard demand or live signal caused it is a bug.

### Operations dashboard

The embedded dashboard is organized around operator decisions instead of raw
metric inventory:

- **Overview** prioritizes actionable health, ready nodes, durable route
  states, hard disk use, model wait, and response-ready-to-wake latency.
- **Scheduler** explains the latest create/hold/stop decision, its reasons,
  hard and leading demand vectors, bounded aging-first wake placement, and the
  exact effective policy used by that cycle.
- **Nodes** combines admission state, actual versus reserved CPU and memory,
  hard disk headroom, memory PSI, storage-operation pressure, and planned wake
  destinations.
- **Sandboxes** and **Registry** retain their operational search, refresh, and
  lifecycle workflows.

Polling is single-flight, pauses while the page is hidden, supports manual or
tunable refresh intervals, redraws charts through one animation frame, and
bounds every high-cardinality table. Session charts are intentionally
in-memory; downloading a snapshot preserves the exact current evidence.

Effective policy is read-only in this iteration. A dashboard write must not
silently mutate production: live tuning remains deferred until there is a
versioned, authenticated policy API with validation, compare-and-set
semantics, audit history, and rollback.

### Phase 1 acceptance

- Static disk, CPU, and memory admission tests continue to pass unchanged.
- Idle nodes never cause live-pressure scale-up.
- Sustained pressure produces at most one additional in-flight headroom node
  per feedback interval.
- A recent-pressure cooldown prevents immediate scale-down oscillation.
- Measured provisioning p95 affects scale-down retention and is visible in the
  dashboard.
- Dashboard polling and autoscaler signal reads remain indexed and bounded at
  100,000 retained events.
- A production canary records scale-up decision time, submit-to-heartbeat p50
  and p95, pressure-trigger count, false-trigger count, ready idle node-hours,
  wake/restore p50 and p95, and storage queue tails.
- The policy is tuned from the canary data before broad traffic.

Program-aware observation and shadow planning may remain deployed with Phase 1,
but queued-wake activation cannot occur until the durable-primary-job flow,
replay, fairness, and crash-boundary gates pass.

The local 20,000-event lower-bound benchmark is recorded in
[`benchmarks/metrics-sqlite-2026-07-31.json`](benchmarks/metrics-sqlite-2026-07-31.json).
Compared with rollback-compatible JSONL, SQLite append was 1.52x faster and an
indexed 1,000-row kind/time query was 16.07x faster. The production mount still
requires its own benchmark.

The program-aware 10x lower-bound benchmark is recorded in
[`benchmarks/program-scheduler-shadow-2026-07-31.json`](benchmarks/program-scheduler-shadow-2026-07-31.json).
At 10,000 requests and 32 candidates, current-state reduction and dashboard
summary stayed below 40 ms median after a 105-ms indexed state read. The full
global plan took 559 ms median and is therefore restricted to the periodic
controller; the per-relay-return shadow choice took 0.6 ms median after the
ordinary route snapshot was available.

## Phase 2: production flow and failure hardening

- Roll out the AgentEnv warm ublk pool at 2/16 idle watermarks and observe a
  sustained SDK/Verifiers park/wake and migration canary. The disposable DFM
  XFS benchmark passed 100 sequential plus 80 eight-way wake/release cycles:
  sequential wake-plus-release p50 fell about 38%, eight-way release p50 fell
  about 65%, 189/189 acquisitions reused warm devices, and final hard
  reservation was zero. The [unpooled](benchmarks/storage-native-ublk-unpooled-2026-08-03.json)
  and [pooled](benchmarks/storage-native-ublk-pooled-2026-08-03.json) results are
  retained; production stability observation remains open.
- Representative private registry images and authentication.
- Cancellation, expiry, gateway restart, Warden restart, ENOSPC, rollback,
  and every drain/migration crash boundary.
- Concurrent wake, migration, and storage-publication saturation.
- Alerts for stale heartbeats, pressure-trigger loops, restore queueing,
  storage errors, reconciliation failures, and leaked ownership.

## Phase 3: legacy task-runtime removal

After the Phase 1 canary and Phase 2 failure matrix:

1. prove legacy admission is closed and legacy inventory is empty;
2. remove the Docker-owned sandbox lifecycle and archive migration path;
3. remove its restore wrapper, flags, capabilities, probes, and tests;
4. retain Docker/containerd image infrastructure.

There must not be a second production task runtime.

## Phase 4: 10x scale qualification

- Thousands of parked routes and published manifests.
- Model-return/wake storms and heterogeneous active resource shapes.
- Registry throughput, cache eviction, storage GC, and remote-fault tails.
- Autoscaler decision and dashboard query cost at retained-metrics limits.
- Packing, drain evacuation, and scale-to-zero behavior with 2-TB workers.

## Later product and optimization work

- Fork/clone over immutable published parents and independent writable COW
  layers.
- Further migration optimization only where profiles justify it. Transfer and
  stage (672 ms) plus source finalize (412 ms) are the current measured targets.
- Optional bounded peer serving only if authoritative registry traffic becomes
  a measured bottleneck; it must not make source sandbox nodes required data
  servers again.
