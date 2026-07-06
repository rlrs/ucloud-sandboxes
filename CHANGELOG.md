# Changelog

This project uses semantic versioning.

## 0.3.21 - 2026-07-06

- Treated fresh zero-sandbox heartbeats as proof that older cached sandbox and
  exec routes are stale, so gateway execs do not proxy to empty or unavailable
  sandbox nodes.
- Returned structured retryable JSON when the routing store is unavailable
  instead of letting SQLite failures drop the request and surface as UCloud HTML.

## 0.3.20 - 2026-07-06

- Added an opt-in `linux_host` sandbox profile for VM-like container startup,
  including root-compatible defaults, writable benchmark harness paths, a
  service shim, optional cron/sshd startup, and keep-alive behavior.

## 0.3.19 - 2026-07-06

- Made `GET /v1/sandboxes` a cached routing-table read by default, with
  explicit `?refresh=true` node reconciliation for callers that need it.
- Persisted sandbox specs and cached states in the routing store so cached list
  responses retain stable ids, images, labels, resources, and node freshness.
- Reduced default `/v1/metrics` work by bounding the event window and caching
  registry summaries unless `?full=true` or `?refresh_registry=true` is used.

## 0.3.18 - 2026-07-06

- Raised the gateway and stdlib node-agent HTTP listen backlog from Python's
  default of 5 to 1024 so UCloud public-link bursts do not overflow the accept
  queue and get reported as `503 Job is unavailable` HTML.

## 0.3.17 - 2026-07-05

- Made sandbox create reservations durable before node-agent create completes, so
  retries do not lose routing state while a container is still starting.
- Kept recent unresolved routes in retryable create-in-progress state instead of
  deleting them and retrying duplicate Docker creates.
- Stopped `/v1/metrics` from synchronously querying node build endpoints and
  bounded node reconciliation calls used by list/recovery paths.
- Increased default sandbox scale-up burst capacity to create up to four nodes
  per cycle, allow eight provisioning nodes, and discount provisioning VM
  capacity until it heartbeats.

## 0.3.9 - 2026-07-04

- Accepted gateway tokens through `X-UCloud-Sandbox-Token` so UCloud public
  links do not intercept sandbox API authentication headers.
- Updated the dashboard to use the public-link-safe gateway token header.
- Serialized heartbeat file access across gateway/autoscaler processes with an
  interprocess lock and unique atomic write files.
- Quarantined corrupt heartbeat JSON and recovered with an empty heartbeat set
  so nodes can repopulate state through normal heartbeats.

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
