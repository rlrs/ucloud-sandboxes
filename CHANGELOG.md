# Changelog

## Unreleased

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
