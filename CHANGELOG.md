# Changelog

## Unreleased

- Made the direct gVisor Warden and storage-native backend the only sandbox
  runtime and durable park/migration path.
- Made deployment identity and the gateway, heartbeat, and node-control
  credentials mandatory and distinct.
- Made role-specific, digest-verified runtime bundles mandatory for node boot.
- Reduced the gateway, node agent, SDK, relay, dashboard, persisted state, and
  configuration to one strict greenfield contract.
- Removed historical runtimes, implicit state conversion, protocol aliases,
  duplicate service assets, planning documents, and alternate migration paths.
