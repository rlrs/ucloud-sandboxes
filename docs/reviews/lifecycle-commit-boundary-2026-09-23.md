# Lifecycle commit boundary

`lifecycle_commit.py` owns worker-receipt validation and the route commit for both
park and wake. It accepts a routing repository, an accepted-heartbeat lookup and
snapshot-reference operations. It has no HTTP handler, status code or response
writer. `LifecycleCommit` returns the committed route and optional program outcome;
typed exceptions distinguish invalid worker proof from a lost route fence.

Snapshot protection precedes the compare-and-swap. On an uncertain commit, durable
route readback determines which dependency closure must survive. Failed readback
releases nothing. A resident wait cannot acquire portable checkpoint metadata.
The HTTP adapter decodes one receipt, maps outcomes to the existing responses and
records the program projection after the authoritative commit.

Seven domain tests use the real routing repository, independently of HTTP, including
stale epochs, competing deletion, commit acknowledgement loss, failed readback and
partial reference protection. Existing HTTP and registry/S3 compensation tests
remain. The focused Linux gate passed 122 tests; the combined rc9 gate passed 1,513
with 12 environment-dependent skips. See
[the regression record](../benchmarks/architecture-tests-2026-09-23/README.md).

This is a bounded extraction. Placement, worker refresh and migration orchestration
still reside in the gateway handler. Their next boundary should return explicit
placement outcomes and preserve the existing shared placement lock and durable
migration repository, rather than introduce another scheduler or move handler
methods into a mixin.

The next bounded slice is `wake_admission.py`: local wake admission now consumes
routes and typed owner views, with heartbeat, placement inventory and canonical
capacity-policy dependencies. It projects accepted wakes before a single atomic
batch reservation, and returns immutable route decisions plus optional shadow
observations. No handler objects enter this domain service. The existing batch
executor and shared placement lock remain; the HTTP layer renders failed single
reservations and handles refresh/relocation when local admission returns no route.

The rc10 focused Linux gate passed 143 tests, followed by all six domain tests
including active-migration exclusion. Tests cover deduplicated reservation,
projected capacity, stale worker/request identity, concurrent deletion, atomic
rollback preserving pending demand, and already-completed wake observation.

The single-reservation readback also closes a pre-existing incarnation race:
losing the CAS cannot report a replacement sandbox as an already-completed wake.
Readback must match generation, create operation and spec hash, and have no
deletion intent. The seventh domain regression performs durable delete/recreate
with each changed identity and verifies that the replacement remains untouched.

Post-rc11 startup hardening validates all retained heartbeat records before the
gateway creates background stores or binds HTTP. An actual server test verifies
both health and fleet listing with legacy nested metrics; malformed retained state
must fail startup before binding. Placement rereads now apply the same incarnation
predicate before accepting a new current route, closing the earlier fallback path
that could bypass the single-reservation fence. These follow-up changes are not
part of the frozen rc11 full-suite count.

## Post-rc12 canonical wake placement

`WakePlacement.place()` now owns the slow wake use case and returns typed
`WakePlaced`/`WakeUnavailable` outcomes. It reuses the existing `WakeAdmission`
local batch path, shared placement reservation and durable migration journal.
The HTTP adapter supplies worker RPCs, capability-aware destination selection
and the existing migration executor; it parses receipts and translates the final
outcome, rather than making refresh/publication/relocation decisions itself.

The use case refreshes only blocked owners, does not publish a multi-GB checkpoint
when no destination can admit it, retains demand during deferral, reserves the
migration before network work, and resumes that same journal on retries. Image
preparation failures now produce a typed domain failure instead of writing an
HTTP response from a nested callback. Legacy private helper entry points that
became test-only were removed; their behavior is exercised through the canonical
adapter or direct domain tests.

Two incarnation fences are explicit: publication cannot replace a concurrent
wake/recreated ID, and completed migration replay cannot mark a replacement
parked incarnation waking. The latter required adding the journal's existing
identity/delete fence to `RoutingStore.complete_sandbox_migration`'s already-
complete branch. Detach rechecks incarnation before contacting a worker.

The migration executor itself is preserved, including its stage journal and
worker protocol. This extraction adds no scheduling authority, queue, transport,
or concurrency controller. Direct use-case tests use real durable stores and
typed heartbeats without instantiating an HTTP handler; gateway integration tests
exercise the same implementation.
