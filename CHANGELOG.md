# Changelog

This project uses semantic versioning.

## Unreleased

## 0.3.39 - 2026-07-11

- Made sandbox termination preempt active exec, SSH, and file-I/O lifecycle leases while fencing new activity until forced runtime removal completes. Fork and restore operations remain strictly exclusive.

## 0.3.38 - 2026-07-11

- Preserved the gateway service account ownership when root-run registry
  maintenance atomically rewrites shared usage state and its lock file.
- Repaired existing registry usage ownership during all-in-one convergence.
- Made control-plane health report `503` when configured registry usage state
  is unreadable, unwritable, or invalid instead of reporting false health.

## 0.3.37 - 2026-07-11

- Generalized the OpenAI model relay into an additional authenticated buffered
  HTTP reverse tunnel with byte-safe bodies, arbitrary ordinary HTTP methods,
  exact encoded paths/query strings, safe header forwarding, and tunnel aliases
  for registration and discovery. Existing OpenAI routes remain compatible.

## 0.3.35 - 2026-07-11

- Restored fork children with raw runsc's detached lifecycle semantics so the
  OCI start call returns to containerd instead of waiting for PID 1 to exit.

## 0.3.34 - 2026-07-11

- Replaced Docker's unsupported cross-container `--checkpoint` start with a
  root-owned `runsc-restore` OCI runtime wrapper. Docker/containerd retain
  child lifecycle ownership while the wrapper durably substitutes raw
  `runsc restore` only for helper-staged fork children.

## 0.3.33 - 2026-07-11

- Made the live-fork probe mutate checkpointed tmpfs files as their owning
  non-root workload user when capabilities are dropped.
- Completed node bundles before switching the gateway package metadata, so a
  running old autoscaler cannot bootstrap a mislabeled mixed-version node
  during an all-in-one deployment.

## 0.3.32 - 2026-07-11

- Fixed the live-fork conformance probe's `/proc/net/tcp` established-session
  match so valid gVisor nodes can advertise `fork-local-v1`.

## 0.3.31 - 2026-07-11

- Added probe-gated, node-local live sandbox forks using gVisor
  checkpoint/restore, durable generation-fenced restore intents, XFS reflink
  checkpoint staging through a narrow privileged helper, same-node gateway
  reservation, and exec/file lifecycle barriers.
- Added the `forkable` sandbox contract, `POST /v1/sandboxes/<id>/forks`,
  same-instant multi-child fan-out with bounded parallel restores, restore-time
  child identity through gVisor's spec environment, mandatory memory/storage
  bounds, and bounded nonce-fenced quiesce/re-key readiness hooks.
- Added startup mark-and-sweep for proven-unreferenced sealed, staged, and
  application checkpoint state; ambiguous pending saves remain fail-closed.
- Bounded retry inspection/readiness in parallel and made post-commit
  checkpoint cleanup best-effort under one wall-clock deadline, so maximum
  fan-out recovery cannot overrun the shared gateway request budget.

## 0.3.30 - 2026-07-10

- Rejected expired model-relay leases at response time, bounded relay admission,
  and cleaned canceled requests from the pending queue.
- Added registry usage generations and cross-process maintenance fencing, plus
  persistent route/build references and finite transient-operation leases that
  fence digest aliases during prune.
  Offline registry GC now restarts the registry even when collection fails.
- Acquired image-use protection before sandbox create/pull dispatch and released
  persistent route references only after a successful matching deletion.
- Added strict autoscaler configuration validation, bounded rotating metrics,
  additive distributed heartbeat persistence, and an independently generated,
  channel-scoped heartbeat credential in generated deployments.
- Added crash-safe cross-process node state files, coherent complete inventory
  heartbeats, a process-lifetime local autoscaler lock, and a compact SQLite
  provider-operation journal.
- Persisted never-reused sandbox route generations, stable create/delete
  operation identities, spec hashes, node/activity epochs, and node tombstones;
  retries now remain bound to the original route incarnation.
- Wired the recurring autoscaler to the durable provider journal, settled
  create visibility guards after exhaustive inventory observation, and retried
  ambiguous immutable-job stops only with fresh same-cycle drain proof.
- Added durable node drain intents, atomic sandbox/build admission closure, and
  token/activity-epoch/zero-work heartbeat acknowledgement before scale-down.
- Added counterfactual drain replanning and a durable cancel/undrain state so
  rising demand cannot execute an obsolete stop; ambiguous undrains remain
  fenced, and autoscaler SQLite/WAL files are owner-readable only.
- Made `reconcile` read-only; `autoscaler-loop --once` is the sole mutating
  one-shot and uses the recurring controller's lock, journal and drain workflow.
- Added node-side aggregate capacity admission and persisted planned create
  intents before runtime mutation so both crash windows remain visible and
  replayable.
- Added bounded in-memory relay admission and request deadlines without
  persisting prompt/response bodies that cannot be reattached to callers after
  a relay process restart.
- Added rollout-incarnation tokens to fence delayed unregister and worker
  operations after a rollout id is reused, and bounded retained relay
  worker diagnostics and completed request/response payloads.
- Added a separate generated node-control credential, constant-time auth on all
  non-health sync/async node routes, authenticated internal clients, and public
  credential stripping at the gateway proxy boundary.
- Made route reconciliation cross-process atomic with exact incarnation
  predicates, strict complete-inventory ingestion, and safe node-epoch adoption.
- Persisted node delete intent before Docker removal and retained
  incarnation-specific pending demand across pre-dispatch/image-pull failures.
- Added persistent managed-registry build/push references acquired before side
  effects and released only on known terminal completion; ambiguous crashes leak
  protection safely until explicit reconciliation.
- Bounded request worker threads and slow client sockets, admitted creates before
  reading their 16 MiB JSON bodies, bounded node file downloads and build/exec
  histories, and avoided redundant node discovery.
- Made registry/UCloud pagination cursor-safe and response-bounded, rejected
  mutating autoscaler fixture inventories, and made sensitive local state
  owner-only with durable atomic writes where applicable.

## 0.3.28 - 2026-07-09

- Reconciled registry pruning with the gateway image metadata cache so deleted
  private-registry tags are also removed from `images.json`.
- Hid stale pushed build records from `/v1/images` when the private registry
  reports that the backing manifest is missing.

## 0.3.27 - 2026-07-09

- Changed scheduled registry pruning to use persistent image last-used state
  recorded by successful sandbox creation instead of image creation time.
- Increased the default scheduled registry retention window from 3 days to 30
  days and kept tags with no usage record out of age-based pruning.

## 0.3.26 - 2026-07-07

- Added scheduled registry retention pruning with a default three-day TTL and
  zero per-repository keep floor so generated one-tag repositories can be
  cleaned up.
- Extended `registry-prune` with `--max-age-days` and wired the all-in-one
  deployment to install and enable the registry prune timer.

## 0.3.25 - 2026-07-07

- Added a dashboard Sandboxes page with live sandbox listing, search,
  per-sandbox termination, and guarded terminate-all controls.

## 0.3.24 - 2026-07-07

- Included active image-build count and build-warm sandbox resources in
  autoscaler cycle metrics so build-driven sandbox warm capacity is explicit in
  dashboard/event data.

## 0.3.23 - 2026-07-07

- Treated pending, prepared, and active image-build work as a transient signal
  to keep or create one default sandbox node, reducing sandbox-node churn while
  images are being built for imminent execution.
- Kept async image-build pending signals until an autoscaler cycle consumes
  them instead of clearing them as soon as a builder accepts the build.

## 0.3.22 - 2026-07-07

- Kept registry dashboard status online when `_catalog` contains a repository
  whose `tags/list` endpoint temporarily returns Docker Registry
  `NAME_UNKNOWN`.
- Marked those partial registry entries as missing tag lists instead of treating
  them as a full registry outage.

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
