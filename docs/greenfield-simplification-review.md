# Greenfield simplification review

Status: historical review, 2026-08-07. Active implementation and remaining
compatibility deletions are tracked in `simplification-roadmap.md`.

This project is greenfield. Runtime compatibility, wire aliases, legacy state
adoption, downgrade paths, deprecated flags, and parallel implementations are
not product requirements. They are defects when they weaken an invariant or
make the current architecture harder to understand.

## Project shape to preserve

UCloud Sandboxes is a provider-aware sandbox control plane. The control plane
owns placement, routes, provider reconciliation, admission and generation
fencing. A direct node agent owns each sandbox lifecycle through the gVisor
Warden. Storage-native volumes and immutable registry artifacts provide park,
wake and migration. The SDK, integrations and model relay use the public
gateway; they do not become alternate authorities for sandbox state.

The implementation remains one deployable control-plane process and a small
node stack. Internal seams follow durable authority boundaries rather than
becoming microservices or a general backend framework.

AgentEnv is a pinned storage implementation and a feature comparison. Its
typed backend boundary, opaque paused state and tagged storage protocol are
useful patterns. Its Firecracker/KVM, template, E2B and fork surfaces are not
the UCloud architecture and are not copied by default.

## Required end state

- `direct` is the only sandbox runtime and storage-native is its only durable
  park/migration layout.
- Production authentication is mandatory and channel scoped. Heartbeats are
  bound to provider-owned node identities and URLs.
- Every mutation has a positive generation, operation identity and canonical
  specification digest. Generation-zero operations do not exist.
- Existing route ownership changes only through an explicit durable create,
  delete, wake or migration transition. Heartbeat inventory observes state; it
  cannot invent a transition.
- Backend device ownership is recoverable across every crash boundary.
- Registry ownership always uses immutable manifest digests.
- Configuration and wire payloads have one spelling, reject unknown fields and
  reject contradictory input.
- Scheduling uses exact protocol/capability requirements rather than a broad
  historical version window.
- One node HTTP implementation and one public vocabulary exist. Sync and async
  SDKs may use different transports but share endpoint semantics.
- Historical conversion is performed by explicit, removable migration tools,
  never silently during normal startup or request handling.

## Accepted findings

1. Heartbeat inventory can replace an existing route merely by reporting a
   higher generation, without a persisted transition or matching incarnation.
2. Optional/fallback gateway, heartbeat and node-control authentication can
   turn a reported node URL into a destination for the private node credential.
3. Storage-native device acquisition activates a pooled device before its
   ownership is durably recorded, leaving a crash window that can strand it.
4. Digestless registry leases follow mutable tags and can protect the wrong
   manifest after a tag moves.
5. Routing startup silently renames any non-SQLite state and starts empty,
   conflating migration, corruption and misconfiguration.
6. Canonical and compatibility resource keys use contradictory precedence, so
   validated capacity can differ from parsed capacity.
7. A broad schedulable-version floor admits nodes missing later correctness
   fixes.
8. The parallel async/legacy node stack duplicates the direct node API and has
   divergent exec streaming and routing behavior.
9. Unknown configuration keys are ignored instead of rejected.
10. Interrupted Inspect cleanup skips closing its original async clients.
11. Node list reconciliation can create a route that the gateway never
    allocated, making an observation an authority transition.
12. Archive migration and generic migration capability flags duplicate the
    storage-native park/wake/migration contract.
13. SDK image builds have both inline base64 and uploaded-context paths, while
    constructors, resource mappings and response decoders accept aliases.
14. The direct image stack retains a flattened Docker-export rootfs pipeline
    beside the production overlay2 materializer.
15. Exec sessions can omit routing identity, and resource quantities silently
    coerce malformed or partial input to zero.
16. Fork, rollout/tunnel registration aliases and provider resume paths expose
    APIs without a current runtime authority behind them.
17. Service units, roadmap/audit documents and benchmark artifacts duplicate
    or describe implementations that are no longer shipped.
18. The control plane can build images locally even though builder nodes are
    the canonical image-build authority.

## Simplification program

### Correctness and authority

- Fence heartbeat boot epochs and route transitions.
- Require distinct credentials and provider-bound node identity.
- Make storage acquisition owner-keyed and crash-recoverable.
- Require registry digests, strict schemas and exact capabilities.
- Fail closed on invalid durable state.

### Compatibility deletion

- Remove the legacy Docker task runtime, async compatibility node agent,
  direct-manager facade and unused direct-node precursor.
- Remove generation-zero operations, optional operation headers, historical
  spec-digest variants, camelCase input aliases and alternate response keys.
- Remove rollout/tunnel duplicate routes, deprecated resume execution, legacy
  state directories, archive migration and downgrade/rollback layouts.
- Remove SDK constructor aliases, unused protocol fields, duplicate packaged
  service assets and stale version declarations.

### Abstraction cleanup

- Keep HTTP handlers thin and place heartbeat admission, routing transitions,
  node proxying and registry lifecycle behind small use-case APIs.
- Express reconciliation as observe, recover, plan, execute and report phases.
- Share pure request construction and response decoding between sync and async
  SDK transports.
- Retain one dashboard stylesheet and one operator workspace hierarchy.

## Completion gates

- Main and SDK Python test suites pass after obsolete compatibility tests are
  deleted or rewritten around the strict contract.
- Ruff and the Go helper build/test pass in a working toolchain.
- No production path contains `legacy`, `compatibility`, `deprecated` or
  fallback behavior except references to external protocol terminology that is
  still current.
- The final change reduces production lines of code and documents any remaining
  intentionally complex authority boundary.

## Implemented resolution

- Replaced the runtime family with `DirectNodeRuntime` and
  `BuilderNodeRuntime`; the direct role requires storage-native ownership and
  the builder role owns image builds.
- Made route generations, operation IDs, spec digests, deployment identity and
  channel-specific credentials mandatory. Heartbeats only confirm an existing
  exact route incarnation.
- Made routing/configuration/resource/bootstrap schemas exact and fail-closed;
  removed startup conversion, alternate field names and generation-zero state.
- Added durable owner identity and active-owner inventory to the pinned
  storage-native backend, and made registry leases immutable-digest keyed.
- Kept one storage-native migration schema/capability and one overlay2 image
  materializer; removed archive migration and flattened Docker export.
- Reduced relay registration to rollout vocabulary, retained arbitrary HTTP
  tunnels as a distinct transport, and removed fork/provider-resume surfaces.
- Made SDK build contexts deterministic and content-addressed, then removed
  inline archives, constructor/resource/response aliases and duplicate client
  cleanup paths.
- Removed duplicate service assets, compatibility modules, historical plans,
  obsolete benchmarks and stale changelog history. Current architecture and
  operations remain documented in the concise canonical guides.

The remaining large modules are the deliberate authority boundaries:
control-plane orchestration, SQLite routing, hibernation recovery,
storage-native ownership and the operator dashboard. They are not generalized
backend frameworks and no alternate implementation sits behind them.

## Validation

- The main suite passes 726 tests; the pinned Verifiers relay integration is
  skipped when its external checkout is unavailable.
- The nested SDK suite passes 74 tests.
- Whole-tree and SDK Ruff checks, Python byte compilation, shell syntax checks,
  Go helper tests and diff whitespace checks pass.
- The pinned AgentEnv owner patch applies after the two existing storage
  patches and passes Rust formatting. Native Cargo compilation remains a Linux
  validation step because the dependency graph uses `io_uring`.
