# Changelog

This project uses semantic versioning.

## 0.3.64 - 2026-07-30

- Added gateway-observed timings for every migration protocol phase:
  source prepare/export, transfer and destination staging, the atomic route
  commit, destination activation, source finalization, and total protocol time.
  The live qualification now reports these separately instead of describing
  the whole protocol as route handoff.

## 0.3.63 - 2026-07-30

- Made a sandbox-create race with node draining explicitly retryable. A node
  now reports its closed admission gate as a definitive rejection, allowing
  the gateway to remove the provisional route, restore pending demand, and
  place the retry on another node instead of pinning it to the draining node.
- Hardened the live SDK/Verifiers migration qualification so route ownership is
  read from gateway node metadata and worker failures immediately fail the run
  rather than leaving a parked harness waiting for its full timeout.

## 0.3.62 - 2026-07-30

- Required a migration destination to materialize the sandbox base image
  before the source enters its fenced `moving_out` phase. This also covers
  relay-triggered wake relocation after the original node has disappeared.
- Put authenticated migration metadata first in new archives and reused the
  parsed manifest during import. Destination admission no longer decompresses
  the multi-gigabyte logical checkpoint repeatedly before extraction.
- Extracted only the authenticated metadata, exact checkpoint inventory, and
  writable upper tree from migration archives before publishing destination
  ownership.

## 0.3.61 - 2026-07-30

- Added a deployment-configured direct-network egress policy for internal
  infrastructure. Sandboxes can reach the exact model-relay IPv4 address and
  TCP port while the broad RFC1918, link-local, loopback, and carrier-grade NAT
  denies remain in force.
- Propagated the auto-detected gateway address and relay port through the
  autoscaler and VM initializer into the single-owner direct-runtime daemon.

## 0.3.60 - 2026-07-30

- Added relay-driven production parking for Verifiers v1 without a guest
  sidecar: durable acceptance parks the exact sandbox generation and committed
  response delivery waits for wake.
- Preserved same-node relay TCP connections across gVisor checkpoint/restore
  with `allow-connected-on-save`, while retaining stable request IDs and
  idempotent reattachment for migration or transport failures.
- Made networked cross-node migration an explicit reconnect boundary without a
  larger gVisor patch: migration archives carry the source guest IP and require
  destinations to allocate a different one, so gVisor closes rather than
  incorrectly reviving TCP through a different host NAT.
- Added durable park/wake transport epochs. A route handoff makes the relay
  publish the accepted request's retry identity before resuming the migrated
  harness, including when autoscaler evacuation happened before wake. The
  same logical retry receives the committed bytes without resampling, while
  ordinary identical calls remain distinct.
- Rebuilt isolated sandbox veth, address, MTU, and route state before restore,
  fixing restored sandboxes that previously came up with loopback only.
- Disabled host-API inactivity parking in production; parking is now an
  explicit relay or client lifecycle decision.
- Reconciled dead restored sentries instead of continuing to advertise them as
  running.
- Qualified the public SDK, production relay, pinned Verifiers `NullHarness`,
  and worker-local interception server through a real
  `running -> parked -> waking -> running` cycle with one model generation and
  one trace turn.

## 0.3.59 - 2026-07-30

- Added a fifth, narrow gVisor patch for restore-start-paused and replay-safe
  resume.
- Made XFS reflink restore hard-quota safe: the Warden durably journals a
  paused candidate, removes the authoritative checkpoint generation, and only
  then allows guest execution.
- Added recovery for crashes on either side of the source-ownership transfer,
  preserving the sole candidate once rollback is no longer possible.
- Selected the single-owner paused handoff for production linear restore. It
  consumes and reuses the checkpoint inode, can return it on failure, never
  duplicates hard-quota usage, and avoids the cold page-cache penalty measured
  for a reflinked memory image. Reflink/prefetch remains an optional future
  fork primitive.
- Replaced the all-in-one installer's fixed two-second startup delay with
  bounded health polling, preventing healthy gateway deployments from being
  reported failed while services are still becoming ready.
- Distinguished an executing stop that is waiting for node drain proof from a
  dry-run stop in autoscaler operator output.
- Cut production over to the direct-only `0.3.59` gateway on DFM compute while
  retaining the established production gateway, relay, private-network, and
  persistent-drive resources. A public SDK scale-from-zero create, exec,
  binary file I/O, idle park, stateful wake, delete, and quota cleanup passed
  against `app-sandboxes.cloud.sdu.dk`.
- Resized the always-on production gateway from the sandbox worker's 32-vCPU
  product to the 2-vCPU/6-GiB product, and made `submit-vm --role gateway`
  choose that smaller product by default while worker nodes remain 32-vCPU.

## 0.3.58 - 2026-07-28

- Authenticated autoscaler-driven drain evacuation against token-protected
  gateways, using the existing gateway credential only for the internal
  migration request.
- Distinguished hard-disk evacuation-capacity blocks from ownership-label
  blocks in human-readable autoscaler output.

## 0.3.57 - 2026-07-28

- Prevented no-net-gain direct-runtime scale-down: a parked disk owner can
  enter drain only when all of its disk shapes fit on ready nodes that remain
  after the same stop batch. The last disk-owning node is retained instead of
  starting a replacement and copying identical state sideways.
- Exposed evacuation-capacity-blocked stop candidates separately in autoscaler
  reconciliation output.

## 0.3.56 - 2026-07-28

- Made the direct OCI runtime pre-create the configured workspace without
  following image symlinks and make it writable inside the sandbox mount
  namespace, restoring SDK file API compatibility for default non-root users.
- Made quota-backed workspace tmpfs mounts explicitly mode `1777` for the same
  non-root compatibility contract.

## 0.3.55 - 2026-07-28

- Added portable direct-runtime parked-state migration with digest validation,
  source fencing, direct node-to-node transfer, atomic gateway route handoff,
  same-generation return fencing, and restart-safe phase retry.
- Changed direct-runtime scaling to physical CPU/RAM and hard physical disk:
  parked sandboxes consume disk only, wake placement reserves active compute,
  fragmented pending shapes are bin-packed per node, and node-side restore
  admission independently prevents active overcommit.
- Made direct-runtime scale-down evacuate parked inventory from a
  drain-admission-closed node before accepting the existing empty-node stop
  proof.

## 0.3.54 - 2026-07-28

- Made capacity preparation parkable-aware so the gateway expands writable
  disk into the same hard checkpoint reservation used by sandbox creation,
  preventing successfully created parkable sandboxes from leaving phantom
  prepared demand behind.

## 0.3.53 - 2026-07-28

- Added the exclusive node-level direct gVisor runtime: one privileged Warden
  owns OCI rootfs provisioning, runsc lifecycle, durable registry and runtime
  identity, tool/file leases, idle parking, restore, deletion, and recovery
  for every sandbox on a node. Docker remains image pull/export
  infrastructure and no longer owns direct-runtime sandbox tasks.
- Added the pinned four-patch gVisor hibernation runtime, two-phase
  quota-backed checkpoints, background restore, restore CPU startup burst,
  exact XFS project accounting, bounded concurrent restores, and fail-closed
  compatibility fencing.
- Integrated direct-only worker bootstrap, bundle-verified runsc delivery,
  scheduler capabilities/accounting, autoscaler deployment flags, clean-node
  ownership, and SDK gateway routing without enabling the legacy task runtime
  on direct nodes.
- Preserved image-root permissions in overlay upper directories and injected
  the trusted init binary into quota-accounted rootfs state, allowing the SDK
  default non-root security profile to create, park, resume, and delete
  sandboxes under gVisor.
- Qualified public SDK create, exec, binary file I/O, automatic idle park,
  stateful wake, delete, and complete ownership cleanup on the live UCloud
  canary deployment.

## 0.3.52 - 2026-07-25

- Made no-capacity sandbox creation responses explicitly retryable with
  `Retry-After`, while combining pending-demand persistence and aggregation in
  one transaction.
- Cached per-node placement accounting for each decision and atomically
  consumed pending demand during allocation, removing redundant route scans.
- Reduced metrics overhead by using read-only aggregate routing queries,
  counting exec sessions in SQL, and coalescing compact metrics responses.

## 0.3.51 - 2026-07-25

- Aggregated saturated sandbox-create admission traces to at most one metrics
  write per second instead of persisting one trace for every rejected retry.
- Made metrics event reads best-effort and non-blocking so dashboard polling
  cannot hold the metrics writer lock on network-backed state storage, and
  reduced the default dashboard event window from 2,000 to 500 events.

## 0.3.50 - 2026-07-22

- Increased the default sandbox-node envelope to 96 GiB RAM and 440 GiB of
  schedulable disk, backed by a 600 GB VM disk, 440 GB Docker quota image,
  2x memory overcommit, and 96 GB of persistent host swap. Builder nodes keep
  their existing 200 GB Docker quota and do not enable managed swap.
- Reported host swap usage and Linux memory-pressure PSI in node heartbeats and
  aggregate metrics. Memory-overcommitted nodes now stop accepting new work at
  sustained full-memory pressure or when RAM plus free swap lacks a bounded
  admission headroom.
- Added explicit per-sandbox Docker swap ceilings and made swap provisioning
  idempotent and fail closed on unsafe live resizes.
- Kept the deployment and packaged systemd units byte-identical and verified
  that the autoscaler CLI accepts the exact arguments emitted by the unit.

## 0.3.49 - 2026-07-12

- Recover autoscaled worker and builder VMs that UCloud suspends after they
  have run. Resume requests use the durable provider-operation journal,
  preserve sandbox routes, are rate-limited and replay-safe, and are confirmed
  from exhaustive job inventory before settling.
- Distinguish UCloud's initial boot-time `SUSPENDED` state from a post-run
  suspension. Post-run suspended VMs now contribute zero projected capacity
  and do not consume create/provisioning limits while recovery is pending.
- Return structured retryable node transport failures: DNS outages are HTTP
  503 `node_dns_unavailable`, timeouts are HTTP 504
  `node_request_timeout`, and other transport failures remain JSON HTTP 502s.
- Persisted all-in-one gateway state on the attached project drive, with a
  one-shot fail-closed migration from job-local state and stable credentials,
  image mappings, registry references, routes, and autoscaler journals across
  gateway replacement. The legacy state path becomes a compatibility symlink so
  package downgrades continue using current persistent state.
- Added generated systemd mount dependencies and mountpoint preflights so the
  gateway, relay, registry, maintenance, and autoscaler services cannot start
  against an unmounted persistent-data path.

## 0.3.46 - 2026-07-12

- Evicted unreachable empty worker VMs after a conservative heartbeat lease
  using a durable provider-stop proof, while preserving route ownership and
  the normal fresh-node drain handshake.
- Stopped replaying successful VM bootstrap on heartbeat loss and isolated SSH
  host keys per UCloud job so recycled SSH gateway ports cannot poison later
  node initialization.

## 0.3.45 - 2026-07-12

- Doubled the default autoscaled sandbox-node product and provisional capacity
  model to 32 vCPU and 64 GiB RAM; builder-node sizing remains unchanged.
- Scored cold-image placement by compressed registry-layer overlap, projected
  in-flight layers, and pull pressure, while retaining exact cached/in-flight
  image affinity and falling back safely when manifest metadata is unavailable.
- Distinguished initial image-cache hits, peer-pull waits, and actual Docker
  pulls in gateway traces, including node-reported Docker pull duration.

## 0.3.44 - 2026-07-11

- Decoupled node-agent protocol compatibility from the gateway package patch
  version. Gateways now accept explicitly supported older 0.3.x node agents,
  allowing gateway-only rolling updates without making the entire live node
  pool unschedulable.

## 0.3.43 - 2026-07-11

- Raised the central gateway HTTP handler limit from 256 to 2,048 and the
  listen backlog from 1,024 to 4,096, providing headroom for 512 concurrent
  rollout long-polls while preserving explicit JSON overload responses.
- Added a gateway CLI override for the HTTP handler limit.

## 0.3.42 - 2026-07-11

- Filtered internal image records by the requested image ID before registry
  enrichment, eliminating registry fan-out and multi-second create admission
  stalls for ordinary unqualified container images.

## 0.3.41 - 2026-07-11

- Bounded serialized sandbox placement admission to 250 ms and reduced concurrent long-running creates from 64 to 32, preventing multi-minute scheduler convoys under burst load.
- Made HTTP worker saturation return an explicit retryable JSON 503 instead of closing accepted sockets and triggering UCloud's HTML `Job is unavailable` response.

## 0.3.40 - 2026-07-11

- Made TTL expiration preempt active exec, SSH, and file-I/O leases so expired sandboxes cannot keep old nodes alive indefinitely.

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
