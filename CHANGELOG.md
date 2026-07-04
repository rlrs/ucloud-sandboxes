# Changelog

This project uses semantic versioning.

## 0.3.7 - 2026-07-04

- Added cached image summaries to node heartbeats so the gateway can prefer
  image-hot sandbox nodes without querying every node image list on each create.
- Extended image pulls with multi-node sandbox prewarm controls.
- Let capacity prepare requests include an image reference for opportunistic
  prewarm on already-ready sandbox nodes.

## 0.3.6 - 2026-07-04

- Moved autoscaled VM Docker storage defaults from the persistent `/work`
  project mount to local VM disk under `/var/lib/ucloud-sandboxes`.
- Kept quota-backed XFS Docker storage while avoiding high-churn Docker layer
  I/O on the network-backed project mount.

## 0.3.0 - 2026-07-04

- Added `deploy-all-in-one` to converge a running gateway VM into the standard
  gateway, relay, registry, and autoscaler deployment.
- Packaged generic systemd unit templates and moved deployment-specific values
  into generated `/etc/ucloud-sandboxes/*.env` files.
- Simplified the all-in-one deployment runbook around the new deploy command.

## 0.2.0 - 2026-07-04

- Added package version reporting to control-plane, node-agent, async node-agent,
  and model relay health endpoints.
- Bounded builder image-build proxy submission requests to 30 minutes.

## 0.1.0 - 2026-06-28

- Initial development release.
