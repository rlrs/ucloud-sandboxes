# Changelog

## 0.5.82 - 2026-09-22

- Upgrade the pinned AgentEnv storage backend to v0.2.2, preserving streamed
  exports, ownership fences, cache identity and warm-device reuse. Include
  cache exhaustion/eviction safety, bounded premerged-index maintenance,
  hybrid discard/rewrite allocation reuse and an explicitly enabled jemalloc.
- Require fresh hybrid writable uppers and validate the complete native patch
  manifest at packaging and boot. Existing workers require sealed snapshot
  migration; do not replace the native daemon in place. Add cross-version
  migration, rollback, full-cache and lifecycle memory qualification.

## 0.5.81 - 2026-09-22

- Queue SQLite writers in arrival order and wake only the next writer. Avoid
  repeatedly waking every blocked request at each commit, while preserving
  grouped durable commits, per-operation rollback and ownership fences.

## 0.5.80 - 2026-09-22

- Commit relay worker poll heartbeats with inference claims, eliminating a
  separate durable transaction per poll. Claim and hydrate batches in one SQL
  statement while preserving registration fencing, distinct leases, ordering,
  and rollback if payload loading fails.

## 0.5.79 - 2026-09-22

- Coalesce gateway routing writes for 5 ms so concurrent lifecycle transitions
  share durable commits. This reduces writer contention without rejecting work
  or acknowledging uncommitted state; worker journal timing is unchanged.

## 0.5.78 - 2026-09-22

- Stop waiting SQLite writers from repeatedly waking one another while a closed
  batch awaits commit. Signal the flusher separately and release writers after
  durability; retain per-operation rollback and generation fences.
- Skip placement simulation for already-running or already-waking sandboxes,
  while still sending the fenced wake to the owner. Keep expected warm-retention
  deferrals out of durable program error writes.

## 0.5.77 - 2026-09-22

- Return a warm-retention retry deadline without holding worker or gateway HTTP
  execution. Preserve the original grace period and durable wake fences across
  retries, and dispatch both parks and wakes using asynchronous HTTP.
- Give durable wakes dispatch capacity independent of park backlog. Release
  deferred lifecycle claims until their next attempt instead of sleeping while
  holding dispatch admission. Keep accepted work queued durably during overload.
- Renew load-test inference leases while deliberately waiting for parking; fail
  promptly if renewal loses ownership. Use subsecond PostgreSQL latency buckets
  to distinguish connection waits, transactions, and durable commit time.

## 0.5.76 - 2026-09-22

- Keep node inventory and heartbeats available when an expired sandbox is still
  owned by a migration. Opportunistic expiry cleanup defers to the fenced
  migration deletion path without discarding the sandbox or its ownership.

## 0.5.71 - 2026-09-21

- Keep bounded local equivalents of successfully published layers using
  hardlinks. Same-worker wakes can read those files without downloading the
  published blobs; eviction preserves active and retired-device pins. Remote
  descriptors remain authoritative, and cache misses use the existing remote
  path. Count cache allocation once per inode instead of sparse logical sizes.
- Reuse completed Registry/S3 layer uploads after a later layer or snapshot
  metadata commit fails. Check immutable input identity and remote blob presence
  before reuse; retry missing blobs normally. Repeated compacted exports can
  also reuse their completed upload without rereading the source stack.
- Compact unpublished local checkpoint layers in the background, independently
  of remote publication. Reuse a dominant local base, retain completed work
  across wakes and appended deltas, and adopt it during a journaled mount or
  publication. Preserve retired-device inputs until release and clean up
  abandoned export hardlinks during reconciliation.
- Stop superseded snapshot exports during streaming for both Registry and S3,
  clean up incomplete uploads, and forward publication ownership checks through
  the backend router. Preserve immutable source layers and final revision fences.
- Remove whole-registry scans from image touches and lease updates. Check registry
  availability without a maintenance write transaction; retain full validation
  and expired-lease cleanup in maintenance snapshots.
- Use indexed lease lookups in gateway image protection, and let sandbox route
  reads proceed during heartbeat writes using committed SQLite snapshots.
  Deleting an absent sandbox no longer loads the entire routing database.
- Receive snapshot uploads into a reusable buffer to reduce copying and temporary
  memory, preserving immutable upload chunks, digest checks, and cancellation.
- Skip queued or retrying relay parks as soon as the model response is durably
  committed; keep already-dispatched parks fenced through completion. Trace
  dispatch queue time separately from lifecycle execution.
- Defer wake-triggered snapshot exports when no destination can admit the
  sandbox, retaining autoscaler demand. Allow busy/draining source workers to
  offload while keeping destination and local-wake admission checks intact.
  Use indexed per-sandbox migration lookup during wake placement.


## 0.5.70 - 2026-09-21

- Retain a dominant published base during depth-only snapshot compaction and
  merge the newer deltas instead. Registry and S3 preserve the existing depth
  and accumulated-delta bounds, with full merges for byte pressure and origin
  changes. This reduces full-base reads and uploads on repeated small updates.

## 0.5.69 - 2026-09-21

- Release relay lifecycle slots during retry backoff, cache validated heartbeat
  decoding, and keep best-effort metrics cleanup from waiting on SQLite readers.
- Avoid repeatedly compacting large snapshot bases; count accumulated delta data
  and allocated sparse-layer bytes while retaining the chain-depth bound.
- Include worker I/O pressure in placement and avoid optional consolidation onto
  more heavily stalled workers. Upgrade the gateway before workers.

## 0.5.54

- Retain generation-fenced node-loss records for seven days. Requests for a lost
  sandbox return HTTP 410 with `error_code: node_lost` and `retryable: false`,
  instead of a generic missing-route response. Existing retained program failures
  are backfilled; a newer sandbox incarnation is never labeled with an old loss.
- Preserve the existing relay behavior: acknowledge retained model responses for
  unavailable callers without replaying model work or claiming a successful wake.

## 0.5.53 - 2026-09-19

- Remove default fleet-wide create and per-worker active-device count ceilings.
  Worker admission queues, disk quota, memory checks, and gateway HTTP/body
  budgets continue to provide backpressure. Explicit operator count overrides
  remain supported; zero disables those optional count ceilings.

## 0.5.40 - 2026-09-19

- Isolate gateway-to-node exec event polling in its own bounded connection pool,
  preserving connections for tools, uploads, and lifecycle operations.
- Report connection-pool admission exhaustion as a safe pre-dispatch 503 instead
  of an ambiguous node transport 502.

## 0.5.39 - 2026-09-19

- Provide HTTP thread headroom for 256 live agent streams and concurrent tools:
  768 gateway request threads and 512 per node, with startup admission unchanged.
- Allow 1024 bounded exec sessions per node, so resident agent processes leave
  room for tool commands and retained results.
- Retain completed exec results for at least 30 seconds under capacity pressure;
  reject new commands before dispatch with safe retry information instead of
  evicting results before their callers can read them.

## 0.5.38 - 2026-09-19

- Preserve early handler rejection responses when clients are still sending
  upload bodies, using the same bounded socket drain as thread-cap rejections.
- Keep device reservations for newly running sandboxes until a heartbeat
  observes their restored devices, preventing concurrent wakes from overbooking.
- Coalesce live capacity refreshes before migrating work away from an apparently
  full owner, avoiding unnecessary publication after a full worker parks.

## 0.5.37 - 2026-09-19

- Preserve structured HTTP overload rejections while clients finish sending a
  request body. Rejected connections now half-close the response and drain
  incoming data with bounded time, bytes, and sockets, without blocking the
  accept loop or consuming request threads. This fixes broken-pipe failures
  seen during a 256-way park burst.

## 0.5.36 - 2026-09-18

- Isolate relay journal work from lifecycle HTTP calls so saturated park/wake
  threads cannot hold up durable responses, leases, and unrelated rollouts.
- Reserve device slots for in-flight wakes and migration destinations before
  heartbeats reflect them; include those reservations in local wake admission.
- Publish local-only parked checkpoints on demand when an owner cannot wake
  them, enabling migration to spare workers without publishing every relay park.
- Bound background checkpoint publication work and consume completed publication
  metadata immediately, with generation and placement fences intact.

## 0.5.35 - 2026-09-18

- Share startup admission across creates, restores and file transfers; reject
  before buffering uploads and keep gateway status/control traffic independent.
- Request bounded early worker headroom for sustained capacity queues, excluding
  reservation age and non-capacity failures, with provisioning credit intact.
- Use targeted sandbox inventory snapshots and cached publication metadata to
  avoid full-node storage RPC amplification during startup polling.
- Return explicit retryable restore/startup rejections before tool execution,
  and pass the scheduler's startup concurrency limit through worker bootstrap.
- Include the deployed relay descriptor-limit and terminal caller-loss fixes.

## 0.5.34 - 2026-09-18

- Balance image builds across live builder load while preserving active-build
  ownership and retry deduplication; report configured builder capacity caps.
- Bound cold-image preparation waits and node connection/pool acquisition so
  provisioning retries release gateway request and create-admission capacity.
- Refresh activity after wake and recheck idle parking under the sandbox lock
  to prevent immediate re-parking before a resumed command starts.
- Add optional consolidation of published parked sandboxes onto occupied
  workers at wake, with pressure/headroom checks, durable migration fencing,
  stable placement order, and a cooldown. Legacy configurations stay disabled.
- Report mixed operation errors and successes without implying recovery, and
  document the production health, capacity, and live restoration checks.

## 0.5.27 - 2026-09-04

- Fenced deletion by sandbox generation and made explicit wake honor node
  admission, live resource pressure, and concurrent restore limits.
- Serialized storage reconciliation against active mutations and introduced
  indexed, paginated live-volume inventory without removing replay tombstones.
- Retained remote storage dependencies across wake and reported them in running
  worker heartbeats; snapshot GC waits for complete dependency metadata.
- Bounded exec input/output by the overall timeout and retained failed build
  completion writes for later persistence instead of leaking build capacity.
- Fixed relay completion pin cleanup and replaced full completion scans with
  incremental indexes; refreshed retention when reusing uploaded build contexts.
- Added cross-component regression coverage and documented review findings and
  gateway-first upgrade requirements.

## 0.5.25 - 2026-09-03

- Made relay-driven parking retry transient lifecycle conflicts uniformly, so
  concurrent SDK status or log polling cannot disable later agent-aware park
  cycles while persistent activity remains bounded by the existing timeout.

## 0.5.24 - 2026-09-03

- Reduced steady-state UCloud autoscaler inventory work to state-filtered active
  jobs, with a periodic deployment-scoped full census and exact transition
  checks, instead of repeatedly paging through the project's job history.
- Avoided reinstalling already configured host dependency packages during the
  verified offline VM bootstrap, removing unnecessary initramfs regeneration
  from the Ubuntu 26.04 cold-node path.
- Unified Registry and S3 snapshot publication concurrency and queue telemetry,
  raised the configurable defaults to four publications and 128 ublk devices,
  and exported publication queue, duration, count, compaction, and byte totals
  through worker heartbeats.
- Split gateway-to-worker proxy latency into response-header and response-body
  spans, added a worker exec-start span with manager phases, and made matching
  image warmups an explicit retryable admission state until usable warm capacity
  exists.

## 0.5.23 - 2026-09-03

- Kept the public gateway on HTTP/1.1 while closing each reverse-proxy-side
  connection after its response, preventing UCloud ingress's idle upstream
  keep-alives from occupying every bounded gateway request thread. Private
  gateway-to-worker polling retains pooled connections.

## 0.5.22 - 2026-09-03

- Identified HTTP admission exhaustion as a guaranteed pre-dispatch fence, so
  coordinated clients can safely retry saturated exec, polling, and cleanup
  requests without treating an already-started mutation as replayable.

## 0.5.21 - 2026-09-03

- Preserved the typed storage-capacity result across the worker's
  storage-daemon Unix protocol, so the worker rollback and gateway requeue path
  introduced in 0.5.20 also applies in the real multi-process deployment.

## 0.5.20 - 2026-09-03

- Classified storage-native hard-capacity and ublk-device exhaustion as
  retryable node admission, rolling back partial worker ownership before the
  gateway requeues or selects another node instead of returning a raw 503.

## 0.5.19 - 2026-09-03

- Closed body-bearing HTTP connections immediately after header parsing and
  consumed valid sandbox-create bodies before admission control, preventing
  pre-body authorization or overload responses from leaving bytes that UCloud
  ingress could replay as the next request.

## 0.5.18 - 2026-09-03

- Prevented body-bearing requests from reusing HTTP/1.1 connections at the
  public gateway and worker server boundaries, so ingress cannot leave bytes
  that corrupt a later create, exec, metrics, or delete request; bodyless
  hot-path polling retains pooled keep-alive connections.

## 0.5.17 - 2026-09-03

- Prevented body-bearing gateway-to-worker requests from reusing HTTP/1.1
  connections, so an early worker response cannot leave unread bytes that
  corrupt a later create, exec, or delete request; bodyless hot-path polling
  retains pooled keep-alive connections.
- Updated the production load harness to construct the current public SDK
  `SandboxSpec` directly instead of relying on the removed keyword shortcut.

## 0.5.16 - 2026-09-03

- Scoped image-use leases to the deployment's managed Registry, so public
  host-qualified images such as GHCR tags no longer fail sandbox creation when
  their external manifests have no managed digest reference.

## 0.5.15 - 2026-09-02

- Made VM bootstrap wait for base-image cloud initialization and reconcile
  pending package configuration before installing the verified offline runtime,
  preventing Ubuntu 26.04 workers from racing `cloud-final` package activity.

## 0.5.14 - 2026-09-02

- Made Ubuntu 26.04 offline worker bootstrap bundles include the complete
  version-locked `util-linux` family instead of mixing base and update-pocket
  packages during installation.

## 0.5.13 - 2026-09-02

- Updated UCloud gateway, builder, and sandbox VM submissions to the live
  `vm-ubuntu:26.04` catalog entry after UCloud retired `vm-ubuntu:24.04`.
- Fenced node restarts with the host boot identity, immutable exec-session
  ownership, and one provider-declared destructive-loss proof contract; UCloud
  guest loss is terminal while recoverable Hetzner power states retain routes.
- Unified route loss, deletion, portable detach, Registry reference cleanup,
  lifecycle observation, and wake eligibility behind single authoritative
  classifiers, including crash-safe migration and publication compensation.
- Versioned fenced park/wake acknowledgements as `hibernate-local-v2`, so new
  parkable placements avoid legacy workers while existing legacy routes fail
  closed until their workers are drained and replaced.
- Reduced gateway and worker hot-path work with exact heartbeat lookup, compact
  JSON, shared short-lived runtime sampling, single-flight image inventory, and
  buffered incremental exec output; the SDK now uses adaptive exec long polls.
- Reported healthy as well as pressured runtime samples in autoscaling metrics,
  separated exec drain leases from create concurrency, and aligned direct-node
  disk admission with the sandbox's actual hard resource claim.

## 0.5.12 - 2026-08-29

- Added an authenticated gateway signal endpoint for attached exec sessions,
  allowing SDK process handles to terminate or kill remote processes without
  conflating signals with stdin or output streaming.

## 0.5.11 - 2026-08-28

- Made park and wake follow AgentEnv's single-flight lifecycle model: concurrent
  transitions join, then re-evaluate the stable runtime state, and repeated
  wake calls against an already-running sandbox succeed idempotently even when
  attached activity is present.
- Centralized the snapshot-publication wake fence in the node lifecycle owner
  and required sandbox-bound relay registrations to declare the managed-agent
  contract, preventing ordinary attached execs from entering agent parking.
- Updated all first-party parking qualification paths to use `start_agent()`
  and `register_agent_rollout()` rather than lower-level job or rollout calls.

## 0.5.10 - 2026-08-28

- Fixed gateway file uploads dropping the request body before proxying to the
  worker, which had produced empty files while returning HTTP 200.
- Kept only file downloads on the streaming-response path and routed uploads
  through the shared body-preserving mutation path.
- Added a production smoke test covering sandbox creation, PEP 723 upload,
  exact byte-for-byte read-back, execution, deletion, and cleanup.

## 0.5.9 - 2026-08-27

- Unified create, wake, exec, and autoscaler admission around measured resource
  pressure, with shared CPU and memory headroom rules and additive disk demand.
- Moved idle parking into the node lifecycle and reserved managed-agent parking
  for the coordinated SDK/relay contract, avoiding competing park decisions.
- Consolidated sandbox HTTP routing, scale-down eligibility, deployment
  convergence, artifact version discovery, and provider runtime profiles so
  every execution path follows the same policy.
- Aligned sandbox networking defaults with the SDK and removed unsupported
  snapshot-publication API surface that could not work end to end.

## 0.5.8 - 2026-08-27

- Made completed background snapshot publication visible through a validated,
  cached worker inventory descriptor without rebuilding it on every heartbeat.
- Acquired permanent snapshot references before granting portable route
  authority, made Registry reference reconciliation exact-key and idempotent,
  and deleted exact routes before releasing their references.
- Restored bounded create-pressure headroom while keeping durable actionable
  demand able to scale toward the configured fleet maximum; publication-only
  waits no longer create ineffective VM demand.
- Required portable authority for remote program-aware wakes, retained local
  wakes for local-only parks, and exposed publication saturation as diagnostics
  without merging it into actionable storage pressure.
- Kept snapshot publication concurrency below the storage operation ceiling so
  wake, mount, release, and delete work cannot be starved by uploads.

## 0.5.7 - 2026-08-27

- Fixed deletion of migrations whose source prepare response was lost: the
  source worker can now recover the unknown snapshot digest from its durable
  moving-out fence while still requiring the exact migration id.

## 0.5.6 - 2026-08-27

- Fixed deletion of successfully migrated sandboxes: activated imports retain
  their migration identity as a storage fence but no longer fail every ordinary
  delete with HTTP 503.
- Split durable-delete retries from the storage-detach budget, allowing cleanup
  backlogs to drain promptly without starving node detachment.
- Added bounded controller reconciliation and metrics for legacy active
  migration journals whose canonical sandbox route is already absent.

## 0.5.5 - 2026-08-27

- Replayed durable sandbox delete intents from the autoscaler so a client does
  not need to remain connected until cleanup succeeds.
- Made gateway deletion cancel an uncommitted storage-native migration before
  retrying the generation-fenced worker delete.
- Terminalized orphaned migration journals when their sandbox route is deleted,
  preventing stale migration reservations from surviving cleanup.
- Added bounded per-cycle pending-delete results to autoscaler observability and
  verified the complete delete, drain, and provider-stop sequence.

## 0.5.4 - 2026-08-27

- Added traffic-independent model-relay maintenance so expired requests and
  worker leases advance even when no client is polling relay state.
- Made park failures explain when attached exec or file activity cannot survive
  a gVisor restore and direct long-lived agents to the checkpoint-owned managed
  process API.
- Stopped lifecycle notification retries immediately for this permanent park
  conflict while retaining bounded retries for transient lifecycle races.
- Documented the coordinated backend/SDK contract for parking-aware agents:
  managed process state and logs survive park/wake, while attached exec
  transports deliberately fence parking.
- Accepted Go RFC3339 nanosecond timestamps on Python 3.10 by retaining their
  representable microsecond precision, keeping managed-agent state portable
  across the supported Python matrix.

## 0.5.0 - 2026-08-13

- Replaced SQLite pseudo-traces with bounded, nonblocking OTLP/HTTP traces and
  metrics, W3C propagation, exporter-health reporting, and a strict schema-5
  telemetry contract.
- Added correlated gateway, worker, relay, exec, image-build, park/wake,
  storage Unix-socket, S3/Registry publication, provider, and VM-bootstrap
  spans, including async context capture and queue-wait phases.
- Enabled AgentEnv's loopback Prometheus endpoint, removed the old dashboard
  trace store, and added overload and microbenchmark coverage proving exporter
  backpressure cannot delay product work.
- Added sampled per-span thread CPU duration, from which the trace backend can
  derive CPU/wall ratios and distinguish wait-heavy phases from optimization
  candidates.

## 0.4.1 - 2026-08-13

- Moved detached sandbox snapshot authority from the gateway Registry to
  Hetzner Object Storage while retaining worker-local NVMe for active COW,
  application memory, and attached parks.
- Added direct bounded-parallel multipart publication, content-addressed
  commits, lost-completion recovery, verification, backend-switch compaction,
  and AgentEnv native S3 range-read configuration.
- Added route-referenced mark-and-sweep snapshot GC, a daily systemd timer,
  incomplete-multipart lifecycle guidance, and strict environment-only S3
  credential propagation.
- Qualified real CPX62 park, detach, compact, cold wake, lazy faulting, and
  full-working-set correctness against the `hel1` Object Storage service,
  including 107.93 MiB/s for a verified 1 GiB publication.
- Added explicit pure-Python dependency injection when repacking a qualified
  node bundle so the S3 runtime remains compatible with golden snapshots.
- Recovered provider-accepted deletes after controller restart so Hetzner may
  safely reuse a deleted worker's private IP without colliding with its stale
  heartbeat binding.
- Defaulted SDK sandbox networking to the production isolated bridge path.

## 0.4.0 - 2026-08-12

- Added a production-shaped Hetzner provider and qualified CPX12 gateways,
  CPX62 workers, Ubuntu 26.04 golden images, private networking, Volume-backed
  registry storage, and end-to-end agentic park/wake behavior.
- Made durably published parked sandboxes portable and detachable so local
  worker disk limits the active working set rather than the total parked
  population, with crash-fenced publication, eviction, and cold wake.
- Upgraded the storage-native backend to AgentEnv v0.1.2 and added streamed
  snapshot-chain compaction, shared bounded remote-layer caching, and corrected
  owner and pooled-device lifecycle handling.
- Split low-latency gateway databases from the Registry blob root, added mount
  fencing for the Volume-backed blob store, and recorded live detached-wake and
  compaction qualification evidence.
- Corrected Hetzner decimal-GB disk normalization, removed worker swap, and
  bounded the CPX62 active storage profile without over-advertising local disk.
- Made the direct gVisor Warden and storage-native backend the only sandbox
  runtime and durable park/migration path.
- Made deployment identity and the gateway, heartbeat, and node-control
  credentials mandatory and distinct.
- Made role-specific, digest-verified runtime bundles mandatory for node boot.
- Reduced the gateway, node agent, SDK, relay, dashboard, persisted state, and
  configuration to one strict greenfield contract.
- Removed historical runtimes, implicit state conversion, protocol aliases,
  duplicate service assets, planning documents, and alternate migration paths.
