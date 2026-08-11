# Simplification roadmap and progress ledger

Status: active implementation, started 2026-08-11.

This document is the durable execution ledger for reducing the UCloud
Sandboxes codebase without removing production features, weakening performance,
or obscuring crash-recovery invariants. It supersedes the completion claim in
`greenfield-simplification-review.md`; that review remains useful historical
context, but compatibility and duplicate-authority surfaces were still present
when this sequence began.

## Baseline

At commit `e803c78` the main repository contained 144,015 tracked lines:

- 67,940 lines in `ucloud_sandboxes/`;
- 33,746 lines in `tests/`;
- 25,510 lines of benchmark-result data in `docs/benchmarks/`;
- 11,293 lines in `runtime/`, including patches, qualification programs, and
  benchmarks.

The separately versioned SDK contained 6,251 production Python lines and 4,099
test lines. The starting verification baseline was 792 passing main tests with
one skipped test, a green 96-test SDK run with 31 optional-integration skips,
and clean Ruff checks in both repositories.

The reviewed first-pass target is a net deletion of 6,000-9,100 production
lines plus 2,000-4,000 obsolete test and benchmark lines. The first concrete
milestone is at most 60,000 lines in `ucloud_sandboxes/` and at most 5,300 SDK
production lines.

## Invariants

Every change in this sequence must preserve these properties:

1. Sandbox generations, operation identities, node epochs, and ownership fences
   remain exact across retries and restarts.
2. Storage admission remains fail-closed and crash-recoverable.
3. Remote registry publication remains content-addressed and revalidated before
   deletion.
4. Migration phases that correspond to distinct remote effects are not
   collapsed merely to reduce line count.
5. Routing state, autoscaler operation state, direct-node orchestration state,
   and storage-daemon state retain separate writer/privilege authorities where
   those boundaries are real.
6. A refactor only counts as simplification when it deletes representations,
   states, modes, or repeated logic. Moving code between files does not count.
7. Greenfield changes do not add compatibility readers, aliases, or migration
   shims. State-breaking changes require an explicit clean-state deployment
   boundary.

## Execution sequence

### Phase 1: compatibility and dead-surface deletion

Target: delete 1,500-2,300 production lines.

- [x] Remove the fixed-upstream, authority-less gateway mode.
- [x] Require content-addressed uploaded image contexts; remove builder-local
      filesystem context builds.
- [x] Delete production-unused and test-only routing, hibernation, exec, image,
      and command-executor APIs.
- [x] Require the canonical hibernation and storage-migration schemas.
- [x] Remove metric/query aliases, stale test adapters, and unlabeled/name-prefix
      ownership adoption where the canonical producer already supplies exact
      identity.
- [x] Replace the remaining generation-zero provisional `SandboxRoute`
      observations with an exact observation type. This is deliberately paired
      with Phase 3's typed reconciliation rather than adding an interim adapter.
- [x] Collapse `plan`, `reconcile`, and one-shot autoscaler compatibility only
      after their canonical command behavior is covered.

### Phase 2: node authority and runtime-mode consolidation

Target: delete 1,700-2,600 production lines.

- [x] Allocate storage accounting identity in the storage-native SQLite
      authority and remove `StorageNativeReservationLedger`.
- [x] Make storage-native ownership and rootfs lifecycle mandatory in Warden;
      remove local-storage, reflink, and obsolete benchmark modes.
- [x] Replace the direct registry JSON/cache/SQLite hybrid with one direct-node
      SQLite authority.
- [x] Consolidate compatible control-plane JSON stores into the control-state
      SQLite database without crossing writer or privilege boundaries.

### Phase 3: typed configuration and orchestration

Target: delete 1,400-2,200 production lines.

- [x] Make one strict deployment manifest authoritative from deployment through
      service startup.
- [x] Delete impossible sandbox overcommit and static-admission settings.
- [ ] Replace generated embedded deployment/init programs with a subtractive
      design. A typed bundle builder/installer prototype was rejected because
      it added 208 production lines; the retained renderer is being simplified
      in place instead.
- [x] Express reconciliation as typed observe, recover, plan, execute, and
      report values with one external serializer.

### Phase 4: shared clients and protocols

Target: delete 1,400-2,000 production lines.

- [x] Share SDK endpoint plans, payload builders, decoders, retries, and handle
      state machines between synchronous and asynchronous transports.
- [x] Replace raw storage transition dictionaries with typed idempotent UCloud
      lifecycle operations while retaining daemon-internal crash states.
- [x] Unify portable hibernation/migration manifest semantics and runtime
      identity.
- [x] Share bounded HTTP mechanics without merging domain-specific error or
      authorization policy.

## Decisions from the audit

- AgentEnv is a comparison for owner-keyed backend acquisition and a single
  orchestrator metadata authority, not a template for UCloud's control plane.
  UCloud registry publication, lifecycle semantics, and crash fencing remain
  authoritative.
- The migration journal phases remain because they fence distinct storage,
  routing, activation, and source-deletion effects.
- `RoutingStore` and `AutoscalerStateStore` remain separate because their writer
  lifetimes and authority differ.
- Builder scaling stays count-based; only pure lifecycle-candidate helpers are
  shared with sandbox scaling.
- Extracting the embedded dashboard into asset files is not counted as a line
  reduction.

## Progress log

### 2026-08-11 - sequence started

- Completed the repository-wide abstraction, minimality, compatibility, and
  authority review.
- Compared the applicable storage ownership boundary against the exactly pinned
  AgentEnv commit.
- Established clean test and lint baselines for the main repository and SDK.
- Began Phase 1 with the fixed-upstream gateway, local image-context fallback,
  and dead/test-only API batches.

### 2026-08-11 - Phase 1 compatibility gate

Canonical paths retained:

- Gateway requests route through heartbeat identity plus the SQLite route
  authority; there is no fixed upstream or authority-less mode.
- Image builds use one uploaded `tar.gz` blob addressed by SHA-256. The gateway
  synchronizes that exact blob to the selected builder before the build.
- `autoscaler` is the only controller command. It is dry-run by default,
  `--execute` authorizes all mutations, and `--once` runs one cycle.
- Provider ownership requires exact deployment and sandbox/builder labels.
  Explicit job IDs remain observation-only rescue inputs and cannot infer a
  role or authorize a stop.

Deleted compatibility and duplicate surfaces:

- fixed-upstream proxy configuration and nullable gateway authorities;
- builder-local context hashing, snapshots, preparation queues, and sync build
  path;
- eight routing facades, hibernation bulk inventory/adoption APIs, async exec
  wrappers, command-executor variants, and other uncalled helpers;
- optional manifest fields, optional migration format, heartbeat defaults and
  coercions, corrupt-heartbeat quarantine-to-empty behavior, relay aliases,
  metric/query aliases, and dashboard field aliases;
- fabricated Docker command results and headers for direct lifecycle and file
  operations;
- `plan`, `reconcile`, `autoscaler-loop`, split execution flags, all-VM/name
  ownership, prefix role inference, and unsafe unlabeled stops.

Realized delta against `e803c78`:

- `ucloud_sandboxes/`: 394 additions, 2,392 deletions, net **-1,998** lines;
- tests: 1,085 additions, 699 deletions, net **+386** lines;
- documentation: net **-9** tracked lines;
- whole tracked main repository: **-1,621** lines;
- SDK production: no net line change in this phase.

The main production package is now 65,942 lines. The first 60,000-line
milestone therefore has 5,942 lines remaining. Test growth in this phase is
explicit debt: canonical archive, routing transaction, strict-wire, and CLI
coverage replaced compatibility tests, but the next passes must consolidate
fixtures and deliver net test deletion.

Intentional compatibility breaks:

- old gateway configuration, controller command names, controller flags,
  unlabeled/name-owned VMs, local builder paths, partial heartbeat payloads,
  missing manifest keys, omitted migration formats, relay aliases, and
  Docker-shaped direct-node diagnostics are rejected rather than translated;
- malformed persisted heartbeat state now stops the controller instead of
  being renamed and interpreted as an empty fleet.

Verification:

- main suite: 792 passed, 1 skipped;
- SDK suite: 96 tests run successfully, including 31 optional-integration
  skips;
- Ruff clean in the main repository; diff whitespace clean in both
  repositories.

Current position: Phase 1 compatibility batches are verified. The exact
observed-route type remains paired with Phase 3. Phase 2 begins with the
storage accounting authority, followed by the single production Warden mode.

### 2026-08-11 - Phase 2 authority checkpoint

Completed authority collapses:

- Storage-native SQLite now allocates the stable accounting ID atomically with
  create/import reservation. The ID is a positive, unique typed column and is
  cross-checked against the canonical record on every load. The socket protocol
  is schema v2 and no caller supplies accounting identity.
- The JSON identity ledger, hibernation disk ledger models, provisioner
  cross-audit, and all reserve/release/inventory call paths are deleted. The
  measured pre-batch production reduction is approximately **574 lines**.
- The direct registry is one strict WAL SQLite authority for registrations,
  generation tombstones, migration tombstones, and activity revision. The
  whole-file JSON cache, sidecar lock/archive, compaction knobs, and atomic
  rewrite machinery are deleted for a net **350 production lines**.
- Three obsolete gVisor Warden/density/node benchmarks were deleted for
  **1,124 runtime lines**. The retained storage-native benchmark now uses the
  v2 accounting API.
- Canonical routing tests were consolidated by **163 incremental test lines**
  without removing transaction-interleaving coverage.

Intentional clean-state breaks:

- unversioned or incompatible storage journals, legacy direct-registry JSON,
  protocol-v1 accounting requests, and removed local benchmark entry points are
  rejected rather than migrated or aliased.

Focused verification:

- storage authority: 115 daemon, quota, provisioner, hibernation, direct-service,
  registry, and migration tests passed;
- direct registry: 52 registry/provisioner tests passed, including concurrent
  first-open initialization;
- routing: all 47 focused tests passed;
- targeted Ruff and diff-whitespace checks are clean.

Warden consolidation:

- Storage-native ownership and rootfs lifecycle are mandatory. `memory_root`
  is the sole artifact root; local-storage, reflink, prefetch, optional-storage,
  and configurable restore-mode branches are deleted.
- Restore always uses the production paused-candidate handoff. Recovery proves
  candidate absence through a successful runtime inventory, keeps ambiguous
  failures mounted in `RESTORING`, and discards a failed restore's COW before
  settling `PARKED`.
- Delete reconciles interrupted hibernate/restore transitions, does not mount or
  traverse settled parked storage, and removes stale control copies on replay.
- This batch removed **624 production lines** and **229 focused-test lines**.
  The 124-test integrated direct/storage run passed; targeted Ruff, format,
  compile, and whitespace checks are clean.

Control-state consolidation so far:

- Registry usage, leases, and references now share three strict SQLite tables
  with generation fencing and short transactions. The JSON/flock/whole-file
  rewrite authority is gone for a net **140 production lines**. A configured
  but invalid database now fails gateway startup rather than starting in a
  degraded compatibility mode.
- Image and image-build state now use one fingerprinted SQLite file with two
  namespaced strict tables and atomic build admission. The mirrored JSON lock,
  load, save, and default-build-file machinery is gone for a net **100
  production lines**. Canonical image tests pass 28/28.
- Legacy JSON, unversioned SQLite, wrong application/version/table layouts, and
  corrupt typed records are rejected without migration readers.

Current position: the storage, direct-registry, Warden, registry-usage, and
image-state batches are focused-test green. Phase 2 full verification remains
pending while direct runtime metadata is folded into its registry and the
remaining heartbeat/bootstrap stores are evaluated against their actual writer
boundaries.

### 2026-08-11 - Phase 2 and first Phase 3 full-suite gate

The direct-node authority boundary is now complete:

- Direct registry schema v2 stores sandbox registrations, tombstones, runtime
  compatibility identity, activity revision, and direct-node drain state in one
  exact SQLite authority. Runtime identity is bound atomically against every
  existing registration before provisioning can proceed.
- `runtime-identity.json`, `direct-node-state.json`, `NodeRuntimeIdentityStore`,
  repeated provisioner binds, and the direct runtime's separate drain store are
  deleted. Builder drain state remains separate because builder nodes do not
  own a direct sandbox registry.
- The metadata consolidation removed another **52 production lines**. It adds
  exact schema/index fingerprinting and concurrent first-open coverage rather
  than weakening validation to meet a line target.

The first Phase 3 compatibility surface is also gone:

- CPU, memory, and disk overcommit factors and the configurable static/dynamic
  admission switch are removed from policy, heartbeat wire state, CLI, deploy,
  VM-init, node agent, systemd, dashboard, metrics, and documentation.
- Dynamic active admission is the one production algorithm. CPU and memory are
  reused under live pressure fencing; disk ownership remains additive and
  storage-daemon bounded. The former `effective_resources` and
  `schedulable_node_resources` identity aliases are deleted.
- Production SQLite paths now use `.sqlite` names for direct registry, storage
  journal, image state, and registry-usage state. No old-path aliases or state
  readers were added.
- This batch removed **371 production lines** and rejects the removed config,
  flags, environment projections, heartbeat fields, and capability marker.

Exact repository position at this gate:

- `ucloud_sandboxes/`: **63,782 lines**, net **-4,158** from baseline;
- tests: net **-238 lines** from baseline;
- documentation plus runtime: net **-1,179 lines** from baseline, including the
  1,124-line obsolete benchmark deletion;
- whole tracked main repository: net **-5,575 lines** from baseline.

The main suite passes all **776 tests** with one skip. Ruff check and diff
whitespace validation are clean; batch-scoped formatting checks are clean.
The 60,000-line package milestone has 3,782 lines remaining. The separately
versioned SDK consolidation is still active and is not included in these main
repository figures.

Current position: core Phase 2 node/storage authorities and runtime-mode
consolidation are full-suite green. Heartbeat/bootstrap control state remains
the last compatible control-plane persistence slice. Work proceeds in parallel
on that slice, typed SDK clients, and the portable hibernation/migration
manifest duplication.

### 2026-08-11 - Phase 4 protocol and shared-client checkpoint

The remaining compatible control state is consolidated:

- Heartbeats and VM-bootstrap records share one strict, namespaced
  `ControlStateStore` SQLite authority. The JSON/temp-file/lock stores,
  namespace-wide save facade, three legacy CLI paths, and split systemd inputs
  are deleted. The production delta is a small but real **-2 lines** and the
  focused tests are **-25 lines**; the value of this batch is one authority and
  one representation rather than a large physical reduction.

Portable runtime identity is canonical:

- Direct registry schema v3 stores one runtime-compatibility digest derived
  from runsc, its commit, and boot configuration. The separate runtime identity
  model/file is deleted, while destination rebind still compares the complete
  runtime fingerprint, including CPU, platform, page size, and rootfs.
- One semantic `HibernationArtifactFile` is shared by local and portable
  manifests; local device/inode identity is composed separately. Hibernation
  manifests and the nested storage runtime schema are v2, with no v1 readers.
- This batch removed **265 production lines** and intentionally rejects the old
  identity stores, manifest layout, stale overlay format name, and migration
  identity field.

External protocol surfaces are smaller and exact:

- Model-relay SQLite no longer duplicates request lifecycle fields beside its
  canonical payload. Worker bodies use exactly one tagged JSON/base64 value,
  and polling now returns only the canonical `requests` batch. Cancellation-safe
  durable publication uses one transition primitive, per-rollout inflight
  counts are derived from authoritative queues, and the test-only synchronous
  close/store seams are gone. The main relay implementation is **183 production
  lines** smaller across these slices.
- Server-side HTTP framing, bounded body reads, JSON/byte responses, and silent
  logging live in one `JsonHttpHandler`; lifecycle, drain, and migration CLI
  calls share one bounded no-redirect JSON transport. Domain authorization and
  exception mapping remain local. This removed **100 production lines**.
- The storage socket protocol is v3. Callers use typed owner/volume records and
  convergent prepare/import/mount/release/publish/discard/delete operations;
  revisions and internal crash states no longer cross the UCloud-facing API.
  Completed operation rows no longer duplicate full volume JSON. The scoped
  package reduction is **66 lines**, and the two retained storage benchmarks
  remove another **129 runtime lines** by deleting revision/dict plumbing.

The nested SDK is independently smaller:

- Sync and async clients share private payload builders, strict decoders, retry
  decisions, direct request mechanics, exec state, and image-build wait state,
  while every async public method remains an explicit typed coroutine.
- Sandbox construction and nested specs are strict and typed. Relay bodies use
  the canonical tagged protocol. Inspect sends the original build context and
  Dockerfile byte-for-byte; runtime rootfs preparation is the sole harness
  directory owner.
- Relay workers now renew and complete only typed polled requests; raw
  request-ID/token overloads and the singular poll projection are deleted.
  Inspect security has one JSON representation, SSH has one typed target, and
  the unused relay-config and persisted-intent compatibility models are gone.
- The unimplemented SSH-WebSocket proxy, redundant tunnel-config object, and
  four unused path-download wrappers are also deleted. The real GET `/ssh`,
  canonical tunnel URL helper, and byte download API remain.
- SDK production is **5,395 lines**, down **856** from baseline; SDK tests are
  **4,054 lines**, down **45**. The full 94-test suite, Ruff checks, formatting,
  and diff validation pass. The 5,300-line milestone has 95 lines remaining.

One proposed change was rejected and fully rolled back: a strict node-bundle
builder/installer added roughly 884 lines while deleting only 676 lines of
generated deploy/init code, a net **+208 production lines**. Moving the
remaining shell into Python would have been code movement, so the repository
retains the previous deployment renderer while a genuinely subtractive design
is sought.

A subsequent dead/compatibility sweep removed another **188 production lines**
and **44 test lines**: the unused sandbox advisory-lock authority and provider
label parser are gone, storage/managed-process compatibility constants with no
consumer are deleted, and UCloud job discovery now has one fail-closed complete
inventory API instead of a configurable partial-browse facade. Test-only image
publication and obsolete direct-registry create/import transition facades were
also deleted; canonical tracked-build and import-plan transitions remain. The
production-only command-executor test fake moved into its sole test consumer.

Exact routing no longer manufactures partially valid domain records:

- `SandboxRoute` construction and schema v2 require a positive generation,
  operation/spec identity, resources, state, and node ownership. Allocation
  uses a separate `SandboxRouteAllocation`, node reports use the existing exact
  `SandboxInventoryEntry`, and incomplete reports protect absence through an
  explicit ID set rather than a generation-zero sentinel.
- Migration capacity uses a typed placement reservation instead of cloning a
  source route under a fake sandbox ID. CAS, epoch, absence, and publication
  fences remain unchanged. This removed **55 production lines**; all 168
  routing/control-plane/metrics/scheduler tests pass.
- Exec-session IDs are opaque UUIDs. Sandbox/node/job identity is stored only
  in the durable exec route, so the unused base64 self-routing representation,
  decoder, and manager identity plumbing are deleted. The 104 focused
  exec/node/gateway tests pass.

Last stable measured repository position before that subsequent sweep and the
active strict-config/route batches:

- `ucloud_sandboxes/`: **63,219 lines**, net **-4,721** from baseline;
- tests: **33,612 lines**, net **-134** from baseline;
- the 60,000-line package milestone has **3,219 lines** remaining.

Focused verification is green: 174 storage/direct/migration/hibernation/node/
relay tests, 172 shared-HTTP/CLI tests, and 94 SDK tests. Ruff and diff checks
are clean on completed batches. A new whole-repository full-suite gate remains
pending after the active strict-deployment-config and exact-route work.

### 2026-08-11 - strict deployment and exact-routing gate

Deployment configuration now has one authority from install through service
startup:

- One exact schema-1 `DeploymentConfig` contains the provider, sandbox and
  builder pools, policy, service ports, retention, and `data_root`. Every state,
  secret, bundle, and SQLite path is derived. UCloud session credentials remain
  an operational input and cannot be persisted in the manifest.
- Deploy installs one `deployment.json`. Gateway, relay, autoscaler, registry,
  prune, and GC units consume only `--config` plus true lifecycle flags. The
  environment files, repeated CLI/config blocks, role sentinels, policy
  overrides, and partial/old schema readers are deleted.
- The exact scoped delta is **13,742 to 11,687 production/systemd lines**, net
  **-2,055**; systemd units account for 104 of those lines. Focused tests are
  **8,397 to 7,615**, net **-782**.

Routing and small residual compatibility surfaces are also closed:

- Exact route allocation/observation/placement removed 55 production lines,
  and opaque exec-session identity removed the unused self-routing encoding.
- A repository-wide call-site audit deleted zero-call state/model/provider
  helpers, test-only VM/public-link projections, redundant image facades, and a
  migration replay facade. The retained TMax adapter was explicitly not
  removed at this checkpoint after its live smoke-script consumer was found;
  the later milestone pass preserves that consumer while deleting the shipped
  general facade.

Repository position before typed provider-recovery consolidation:

- `ucloud_sandboxes/`: **60,748 lines**, net **-7,192** from baseline;
- tests: **32,820 lines**, net **-926** from baseline;
- the first package milestone has **748 lines** remaining.

The strict deployment full gate passed **758 tests** with one skip. The newer
exec, route, image, model, provider, and direct-service focused gates are green;
Ruff, compilation, formatting, and diff-whitespace checks pass. Work proceeds
to the single typed provider inventory-recovery/reporting result.

### 2026-08-11 - first package and SDK size milestones crossed

Reconciliation and its external report now have one typed path:

- Provider inventory is reconciled in one pass. `ProviderOperationOutcome`
  remains typed through replay, capacity accounting, safety decisions, and the
  final serializer. Four recovery/confirmation APIs, the second recovery
  model, blank-role handling, and duplicate create/stop result projections are
  deleted for **61 production lines**.
- Sandbox and builder creation use one role-parameterized intent builder and
  the typed `InstanceCreateIntent` directly. Duplicate create counts, stop
  helpers, label construction, raw intent arrays, and report-only payload
  projections are gone for another **104 production lines** and **20 test
  lines**.

The last duplicate authority/protocol adapters in this pass are also gone:

- The storage daemon's typed volume record is the provisioner's sole quota and
  accounting authority. The 197-line quota projection, provisioning result
  envelopes, and three repeated journal completion transactions are deleted;
  the scoped production reduction is **208 lines**.
- Gateway and node build-context ingestion share one bounded,
  content-addressed handler. Pending-demand expiry, collection path parsing,
  path-keyed route caches, resource arithmetic, metric-event insertion, and
  runtime-metric decoding each have one canonical implementation.
- Runtime metrics require the complete canonical field set and exact JSON
  types. Partial records, numeric strings, bool-as-number, negative counters,
  and non-finite values are rejected; this removed **76 production lines**.
- VM init reads and validates `package-bundle.json` once. Package, kernel,
  runsc, managed-init, storage-native, AgentEnv commit, and patch provenance
  checks remain exact, while the five embedded readers and unused shell
  projections are removed for **71 production lines**. This is the subtractive
  replacement for the rejected bundle-installer prototype.
- Forty-nine complete dashboard CSS rules whose selectors cannot be produced
  by the HTML or JavaScript were deleted for **294 production lines**. Mixed
  selectors and every dynamic status, health, navigation, and severity family
  remain.
- The service package no longer ships the 249-line general TMax facade. Its
  sole live consumer, the repository smoke runner, now owns a 107-line private
  adapter with byte-identical generated artifacts. Unused tag, registry,
  file-mapping, and generalized parsing APIs are gone; the whole-repository
  scope is **155 lines** smaller.

The SDK milestone is complete as well. One typed sandbox constructor and SSH
target remain; stale SSH creation/refresh/proxy conveniences are deleted. Relay
completion accepts one JSON-or-bytes body representation, while sync and async
public I/O methods remain explicit. Current SDK production is **5,296 lines**,
down **955** from baseline; SDK tests are **4,058 lines**, down **41**, and all
94 tests pass.

Exact repository position at this milestone:

- `ucloud_sandboxes/`: **59,602 lines**, net **-8,338** from baseline;
- tests: **32,674 lines**, net **-1,072** from baseline. Sixty-five identical
  control-plane server lifecycle blocks now use one fixture while all 90 test
  methods and 436 assertions remain; the VM-init corruption fixture retains
  all 16 cases in one data-driven table;
- runtime sources, patches, and retained benchmarks: **9,985 lines**, net
  **-1,308** from baseline;
- main package plus SDK production: net **-9,293 lines**.

Both first milestones are therefore crossed: the main package is below 60,000
lines and the SDK is below 5,300. The final integrated main suite passes **759
tests with one skip**; the SDK passes all **94 tests**; the Go helper passes its
package test with external linking on the local macOS toolchain. Ruff lint and
diff-whitespace checks are clean. The formatter reports only 23 files that were
already unformatted at the baseline commit; every newly introduced formatting
regression is fixed. Work can proceed from this green checkpoint to the next
independently subtractive production batch.

## Per-batch completion record

Each completed batch must record:

- the canonical path retained;
- the deleted modes/APIs and realized line reduction;
- state or wire compatibility intentionally broken;
- focused and full verification results;
- the next uncompleted item in the sequence.
