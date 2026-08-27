# Changelog

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
