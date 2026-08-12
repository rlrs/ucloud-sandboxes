# Changelog

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
