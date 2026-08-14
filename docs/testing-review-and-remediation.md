# Test-suite review and remediation ledger

Status: remediation, subtractive, and behavior-architecture passes implemented
and locally validated on 2026-08-14. The infrastructure-dependent tiers listed
below remain outstanding.

This document persists the repository-wide test review and tracks the work
needed to turn its findings into simpler, stronger tests. The central finding
is that the suite is not filler-heavy: its SQLite, filesystem, socket,
restart, recovery, and concurrency tests provide substantial confidence in
the durable control logic. Confidence is weakest at real system boundaries,
and a smaller group of tests asserts source text or private implementation
details instead of public behavior.

Property-based testing is additive here. Named regressions remain the clearest
way to preserve known crash, race, and security failures. Generated examples
and model-based state machines are better for the combinatorial rules that the
current hand-written examples do not delimit. Real contract tests remain
necessary for Docker, gVisor, S3, registry, provider, VM, SDK/gateway, and
Python/Go boundaries.

A checked item is implemented across the scope stated by that item. Unchecked
items may have substantial partial work; those cases are called out explicitly
so the ledger does not erase progress or overstate completion.

## Reviewed baseline

This section records the pre-remediation baseline, not the current suite.

- Main package: 852 tests passed in 53.1 seconds, with one optional integration
  skipped; measured statement coverage was 79%.
- Nested SDK: 94 tests passed in 17.0 seconds in the populated workspace;
  measured statement coverage was 82%. A minimal dependency installation can
  silently skip its 28 Inspect tests.
- Managed-process Go helper: three tests passed with external linking. The
  default macOS linker failed before test execution because its output lacked
  `LC_UUID`; Linux remains the production validation platform.
- No Hypothesis dependency or property-based tests were present.
- `ucloud_sandboxes/direct_runtime.py` and
  `ucloud_sandboxes/storage_native_service.py` had no executed statements in
  the main coverage run.
- The documented root test command covered neither the nested SDK nor the Go
  helper, and no in-repository CI workflow was present.

Coverage percentages are diagnostic only. They do not distinguish useful
state-transition assertions from assertions that merely execute code.

## Tests that define valuable behavior

Retain the named regressions around these boundaries:

- routing generations, migration journals, restart behavior, and competing
  writers;
- storage-native lifecycle revisions, accounting identity, capacity,
  reconciliation, and interrupted create/release recovery;
- hibernation manifests, irreversible journal boundaries, symlink rejection,
  and crash recovery;
- registry digest ownership, leases, tag movement, and delete serialization;
- model-relay commit/cancellation atomicity, durable response pins, restart,
  stale-incarnation fencing, and HTTP byte/header forwarding;
- control-plane authentication, channel scope, persisted routing before slow
  creation, ambiguous create recovery, bounded proxying, and
  content-addressed builder flows;
- real socket framing and overload behavior;
- build-context CAS repair, atomic publication, concurrent reservation, and
  garbage collection;
- direct provisioner and Warden durable phase recovery and failure injection;
- SDK redirect credential protection, bounded responses, deterministic build
  contexts, retry safety, and ambiguous-operation recovery;
- the Go managed-process subprocess and Unix-socket tests.

These are component tests rather than complete deployment tests, but they
exercise the real authority and persistence algorithms. Replacing them with
mock-only unit tests or generated examples would reduce confidence.

## Confirmed correctness and safety findings

- [x] Make VM-init authorized-key rendering structurally immune to fixed
  heredoc termination and shell injection. Add a named regression and
  generated shell-hostile inputs.
- [x] Prevent registry pruning from deleting a digest while any alias remains
  ineligible. Cover mixed-age and mixed-lease aliases through the production
  execution path.
- [x] Revalidate or fence S3 snapshot-GC candidates at execution so an object
  that becomes referenced after planning cannot be deleted.
- [x] Make `image_id_from_tag` always produce a valid image ID and avoid
  deterministic truncation collisions. Test validity, determinism, length,
  content sensitivity, and hostile references.
- [x] Validate relay renewal identity completely. The SDK fake previously
  returned a different rollout ID and the client accepted it, demonstrating
  that the replica can drift from the real relay contract.
- [x] Unify SDK package/runtime version reporting; metadata was `0.4.8` while
  `ucloud_sandboxes_sdk.__version__` was `0.4.7`.
- [x] Serialize first-open registry-usage schema inspection. Two processes
  could previously observe `user_version` and tables from different SQLite
  snapshots and reject a valid database intermittently.
- [x] Keep the managed-process socket protocol bounded and strict without
  requiring clients to half-close before the supervisor responds.

## Tests to simplify or replace

- [ ] Keep dashboard HTML/ID/parse smoke tests, but replace source-token, CSS
  comment, animation-name, and fixed-tab-count assertions with browser and
  accessibility behavior tests.
  Partial: the source-token, CSS-comment, animation-name, and fixed-count
  assertions were removed or weakened to structural accessibility checks;
  browser behavior and accessibility coverage remains deferred.
- [x] Replace telemetry and benchmark source-string checks with executed
  transport tests that capture emitted traces.
- [ ] Retain shell syntax checking, but replace brittle substring assertions
  with ShellCheck, parsed configuration/unit assertions,
  `systemd-analyze verify`, and a disposable-host smoke test where practical.
  Partial: Bash parsing and required-by-default ShellCheck are in the canonical
  check. Parsed-unit, `systemd-analyze`, and disposable-host verification remain.
- [x] Rewrite the configuration secret-persistence test so the asserted
  secret actually enters the input path.
- [ ] Rename tests whose names claim atomicity, aggregation, offset checking,
  bounds, or live socket preservation when their assertions only cover error
  mapping or command construction. Add the missing behavior where it is a
  supported contract.
- [ ] Replace wall-clock sleeps and upper-bound timing assertions with events,
  barriers, failpoints, and injectable clocks.
  Partial: the storage, registry, provisioner, and relay timing proxies found
  during this review were removed or replaced with deterministic coordination;
  the unchecked status reflects that this has not yet been enforced suite-wide.
- [ ] Assert public quiescence and resource cleanup instead of private dicts,
  exact SQL transaction counts, heap thresholds, write counts, or inode/mtime
  preservation. Retain a small canonical set of exact adapter-command tests.
  Partial: private relay heaps and dictionaries, exact SQL/write/call counts,
  storage internals, and several exact command probes were removed. Some legacy
  adapter and fake-state assertions remain outside the reviewed clusters.
- [ ] Consolidate repetitive strict-schema rejection cases into canonical
  table-driven field mutations without weakening fail-closed behavior.
  Partial: registry, control-plane, VM-init, and SDK cases were consolidated;
  other serializers still use bespoke examples.
- [x] Consolidate historical CLI tombstones into a strict public-command
  allowlist rather than preserving one test per removed spelling.

## Property and model test program

- [x] Add Hypothesis as a development/test dependency with deterministic CI
  settings and an explicit example-database/reproduction policy.
- [ ] Model registry prune and S3 GC over generated aliases, digests,
  timestamps, references, leases, plans, and input permutations.
  Partial: generated alias eligibility and plan/protection sets now exercise
  reordering and revalidation, but the full multi-digest, timestamp, and lease
  model remains open.
- [ ] Add a `RoutingStore` state machine covering allocate, update, delete,
  migrate, reconcile, and reopen with generation and non-resurrection
  invariants.
  Partial: allocate, update, delete, replay, stale identity, monotonic
  generation, non-resurrection, and reopen are covered; migrate and reconcile
  remain.
- [x] Add a storage-native service/Warden state machine covering lifecycle
  operations, restart, reconciliation, and injected durable-boundary failure.
- [x] Add a model-relay state machine covering register, enqueue, poll, renew,
  respond, unregister, time advancement, and restart.
- [x] Check autoscaling and placement decisions against a small exhaustive
  feasibility oracle, including input permutation and monotonic-capacity
  properties.
- [ ] Generate hostile archives, paths, image references, JSON schemas,
  framed responses, pagination graphs, and `/proc` samples. No malformed input
  may escape its destination, bypass a bound, or crash a parser unexpectedly.
  Partial: generated archives, paths, image references, relay bodies, Go
  frames, and `/proc` samples are covered. Generated pagination graphs and
  broader schema families remain.
- [ ] Run shared generated scenarios against synchronous and asynchronous SDK
  transports to enforce semantic parity.
  Partial: generated poll request construction now enforces sync/async parity;
  shared lifecycle and failure scenarios remain.
- [x] Add Go fuzz targets for managed-process request decoding, validated
  specs, and response framing.

Small finite recovery classifiers should use exhaustive truth tables instead
of randomized generation. Crash consistency and races should use deterministic
failpoints and process orchestration rather than relying on scheduler timing.

## Test ownership rules

These rules are the basis for future additions and for the aggressive
consolidation recorded below:

- A public behavior has one primary owning tier. CLI tests own parsing, wiring,
  safety fences, and complete operator workflows; they do not duplicate policy,
  routing, provider, or persistence algorithms.
- State machines own combinations of durable transitions, replay, restart, and
  stale identities. Named examples remain only for distinct incidents, crash
  boundaries, security failures, or unusually diagnostic edge cases.
- Property tests own broad input spaces and algebraic rules. A new fixed example
  must demonstrate a boundary not already expressed by the property or serve as
  a readable regression for a real defect.
- Real HTTP, process, socket, registry, and object-store contracts own transport
  behavior. Mocked request choreography should be limited to failures that a
  local contract cannot deterministically induce.
- Exact SQL, cache, queue, call-count, timing, argv, and generated-text assertions
  are not contracts unless an external protocol makes that exact representation
  observable. Prefer resulting state, resource safety, and operation ordering.
- A table-driven scenario should represent error taxonomies and equivalent
  variants. Separate tests are warranted when setup, recovery, or the expected
  invariant is materially different.

## Contract and integration test program

- [ ] Add an always-on contract suite between the real gateway/relay app and
  the nested SDK; keep only small scripted transports for unit-level request
  construction and decoding.
  Partial: real HTTP gateway and relay contracts against the pinned SDK peer
  are always-on in CI; consolidation of the remaining large fake services is
  still open.
- [x] Add a Linux Python-to-Go managed-process compatibility test.
- [x] Add local registry and MinIO/S3 tests for real HTTP, upload, pagination,
  aliasing, error, and same-origin behavior.
- [ ] Add a privileged Linux tier for Docker overlay mounts, AgentEnv storage,
  namespaces, and actual park/wake cleanup.
- [ ] Add a nightly pinned-gVisor tier for checkpoint/restore, live socket
  preservation, migration, and network isolation.
- [ ] Add disposable VM/systemd verification and opt-in live-provider smoke
  tests.
- [ ] Add Playwright behavior and accessibility coverage for the operator
  dashboard.

## Suite structure and speed

- [ ] Run test HTTP servers with a short shutdown poll interval and centralize
  their lifecycle helper. The default 0.5-second `serve_forever` poll dominates
  dozens of main and SDK tests.
  Partial: affected servers now use a short poll interval; lifecycle helpers
  are not yet centralized.
- [ ] Split `tests/test_control_plane.py` by authentication, placement,
  lifecycle, proxying, registry, and builder behavior.
  Partial: the file was reduced from 5,131 lines and 71 tests to 3,597 lines and
  41 representative vertical tests. Physical splitting remains useful for
  navigation but is no longer required to remove duplicated subsystem coverage.
- [ ] Keep CLI tests focused on parsing and wiring; move orchestration
  scenarios behind reusable application/service harnesses.
  Partial: the CLI suite is now 1,849 lines and 17 vertical wiring, recovery,
  fencing, and destructive-operation workflows, down from 4,833 lines and 69
  tests before remediation. Further extraction is a maintainability choice,
  rather than a prerequisite for eliminating policy/provider duplication.
- [ ] Replace the SDK's large semantic fake services with a concise scripted
  request/response transport plus a small set of real-socket tests.
  Partial: Inspect create/build behavior now uses two table-driven scenario
  clients, mirrored cases were removed, and the real relay/gateway contracts
  remain. The general SDK client fake still covers several unrelated endpoints
  and can be replaced incrementally by the real contract tier.
- [ ] Parameterize shared synchronous/asynchronous SDK scenarios instead of
  maintaining partially duplicated test bodies.
  Partial: generated poll construction is shared and redundant mirrored cases
  were removed; representative sync and async retry/lifecycle paths remain
  separate for readable failures.
- [ ] Split large multi-concern snapshot and lifecycle scenarios where a
  failure currently obscures which invariant broke.
  Partial: storage, hibernation, provider, SDK retry, and CLI families now share
  scenario harnesses or tables. A few intentionally end-to-end crash and
  content-addressed build tests remain large.

## Validation and continuous integration

- [ ] Add one repository test entry point that runs the main Python suite, SDK
  minimal-install suite, SDK `inspect` extra suite, Go tests, Ruff, shell
  syntax/ShellCheck, and generated-unit verification where supported.
  Partial: the canonical check now requires explicit opt-outs, builds and
  smoke-tests both installed wheels, and runs the Python, SDK, Go, Ruff, Bash,
  and ShellCheck tiers. Generated-unit verification remains.
- [x] Run fast unit, property, and local component tests on every change.
- [ ] Run Linux contract tests separately and privileged/live tests on an
  explicit or nightly schedule.
  Partial: registry and MinIO contracts run separately on Linux and Go fuzzing
  runs on the schedule. Privileged, gVisor, VM, and live-provider tiers remain.
- [ ] Run targeted mutation testing on pure policy, prune, configuration,
  metrics, parser, and networking modules; use surviving meaningful mutations
  to guide test deletion and replacement.
- [ ] Record final suite counts, coverage by testing tier, runtimes, and any
  platform-only validations after the remediation is complete.
  Partial: current test counts and local runtimes are recorded below; coverage
  was not rerun, and the Linux/container/live tiers remain environment-gated.

## Progress log

### 2026-08-14 - review completed

- Inventoried and executed the main Python, nested SDK, and Go helper tests.
- Reviewed tests by subsystem for observable behavior, recovery value,
  implementation coupling, fake-boundary fidelity, and unnecessary timing.
- Reproduced four defects currently missed by the suite: VM-init heredoc
  injection, shared-digest registry deletion, stale-plan S3 deletion, and
  invalid/colliding derived image IDs.
- Started implementation with the confirmed safety/correctness findings.

### 2026-08-14 - first remediation pass implemented

- Fixed and regression-tested the confirmed correctness/safety findings:
  shell-safe authorized keys, digest-wide registry pruning, S3 execution-time
  revalidation, collision-resistant valid image IDs, complete relay renewal
  identity validation, one-source SDK version reporting, atomic registry-usage
  initialization, and backward-compatible bounded Go request decoding.
- Added deterministic Hypothesis profiles, generated parser/configuration and
  lifecycle properties, bounded routing/storage/relay state machines, an exact
  placement oracle with permutation and monotonicity checks, and three Go fuzz
  targets. The partial-model caveats above remain intentional follow-up work.
- Added real-process build-context GC concurrency coverage, cross-process
  registry lease/prune fencing, real HTTP registry-client contracts, Docker
  Distribution and MinIO contracts, SDK-to-gateway/relay contracts, and the
  Linux Python-to-Go supervisor contract.
- Removed or consolidated brittle dashboard, telemetry, configuration, CLI,
  timing, and duplicate-schema assertions where a stronger observable
  replacement was available. The architectural cleanup items left unchecked
  above were not treated as prerequisites for this pass.
- Added `scripts/check.sh` as the canonical check, installed-wheel smoke tests
  for both distributions, locked Python 3.10/3.13 CI matrices, required
  ShellCheck with explicit local opt-outs, bounded workflow timeouts, cleanup
  steps for contract containers, a pinned SDK peer revision, and scheduled Go
  fuzz runs.
- The canonical runner now creates separate temporary project environments for
  the root package and SDK, so concurrent Python-version runs cannot recreate
  a shared `.venv` underneath one another.
- The full canonical pipeline passed under both CPython 3.10.13 and 3.13.5.
  Each leg ran 896 root tests (4 environment-gated skips), built and tested the
  installed root wheel, passed Ruff, Bash, ShellCheck, and the Go suite, built
  and tested the installed SDK wheel, and ran 110 SDK tests. Root tests took
  20.860 seconds on 3.10 and 21.368 seconds on 3.13; SDK tests took 1.327 and
  1.351 seconds respectively on this host.
- The synchronized registry first-open regression additionally passed 50
  consecutive spawned-process runs on CPython 3.13 after the transaction fix.
- The root compatibility pipeline was also exercised against its exact pinned
  SDK commit: installed-wheel smoke, scoped Ruff, and all 94 tests passed on
  CPython 3.13. The SDK's own changed worktree remains the authority for the
  new 110-test SDK suite.
- This macOS host has no Docker executable, so the Distribution and MinIO tests
  remained among the explicit skips locally. The Ubuntu CI job starts pinned
  containers and runs both contracts; privileged Linux, gVisor, VM, browser,
  and live-provider tiers remain deferred.

### 2026-08-14 - subtractive quality pass

- Reviewed the enlarged suite specifically for marginal confidence per test.
  Kept named security, corruption, crash-boundary, race, fencing, recovery, and
  destructive-safety regressions, plus the new state machines, exhaustive
  placement oracle, hostile-input properties, and real protocol contracts.
- Removed examples subsumed by those stronger tests: private query plans and
  caches, exact call/transaction/write counts, timing thresholds, generated
  script substrings, repetitive schema mutations, duplicated happy paths,
  presentation-only CLI checks, and mirrored SDK fake-transport cases.
- Consolidated root tests from the post-remediation peak of 40,040 lines and
  896 executed tests to 35,714 lines and 745 tests: 4,326 fewer lines and 151
  fewer tests. This is also 652 lines and 107 tests below the original reviewed
  baseline, despite the added property, model, process, and HTTP contract tiers.
- Consolidated SDK tests from the post-remediation peak of 4,775 lines and 110
  tests to 4,057 lines and 85 tests: 718 fewer lines and 25 fewer tests. This is
  two lines and nine tests below the original SDK baseline.
- Combined, the subtractive pass removed 5,044 test lines (11.3%) and 176 tests
  from the post-remediation peak. The resulting two suites are 654 lines and
  116 tests smaller than the original baseline.
- Full discovery passed on CPython 3.10.13 and 3.13.5 with 745 root tests and
  the same four environment-gated skips on each interpreter; each run took
  about 21.6 seconds. All 85 SDK tests passed on CPython 3.14.3 in 0.9 seconds.
  Root and SDK Ruff lint, diff whitespace validation, and the externally linked
  Go supervisor suite also passed. No dependency synchronization was performed.

### 2026-08-14 - aggressive behavior-architecture pass

- Assigned each behavior to one primary tier using the ownership rules above,
  then removed secondary copies. The largest reductions came from CLI policy
  duplication, control-plane endpoint variants, storage/registry happy paths,
  routing and relay examples already represented by state machines, and SDK
  retry tests that each carried a bespoke fake client.
- Replaced families of examples with stronger coverage: an exhaustive placement
  oracle, routing/storage/relay state machines, a generated program-demand
  reducer, table-driven provider and failure classifications, shared SDK
  create/build recovery clients, and real loopback registry behavior replacing
  mocked private request choreography.
- Root tests fell from 35,714 lines and 745 executed tests at the start of this
  pass to 27,371 lines and 545 tests: 8,343 fewer lines (23.4%) and 200 fewer
  tests. SDK tests fell from 4,057 lines and 85 tests to 2,991 lines and 53
  tests: 1,066 fewer lines (26.3%) and 32 fewer tests.
- Combined, this pass removed 9,409 lines (23.7%) and 232 tests. Compared with
  the original reviewed baseline, the resulting suites are 10,063 lines (24.9%)
  and 348 tests (36.8%) smaller while retaining the new property, model,
  process, fuzz, and real-protocol tiers. Compared with the post-remediation
  peak, they are 14,453 lines (32.3%) and 408 tests (40.6%) smaller.
- The remaining named regressions emphasize authentication, bounds, traversal,
  corruption, crash recovery, durable ambiguity, ownership, stale-generation
  fencing, non-resurrection, data-loss prevention, and deterministic races.
  Exact internals remain only where they represent an external adapter or wire
  contract that cannot yet be exercised on this host.
- Full discovery passed on CPython 3.10.13 and 3.13.5 with 545 root tests and
  four environment-gated skips in 18.0 and 17.8 seconds respectively. All 53
  SDK tests passed on CPython 3.14.3 in 0.7 seconds. Root and SDK Ruff lint, Go
  supervisor tests, shell syntax, and diff whitespace validation also passed.
  Coverage was not rerun because the existing environments do not contain a
  coverage collector; the retained-behavior inventory above is therefore the
  confidence measure for this pass rather than an unverified percentage.

### 2026-08-14 - coverage-preserving shared-harness pass

- Kept every remaining named test and behavioral assertion. This pass did not
  delete or merge cases; all reductions came from shared setup, transport, and
  lifecycle fixtures.
- Control-plane tests now share temporary-root, gateway construction, and
  URL-yielding server lifecycle helpers. CLI autoscaler workflows share their
  canonical provider job, heartbeat, argument, temporary-root, and reconcile
  setup instead of rebuilding the same durable environment inline.
- SDK tests now use one scripted synchronous/asynchronous response transport
  and shared Compose build-context/launch helpers. Storage Registry and S3
  publisher tests use one exporter that still crosses the real Unix-socket
  streaming boundary.
- Root tests fell from 27,371 to 26,844 lines while retaining 545 discovered
  tests. SDK tests fell from 2,991 to 2,861 lines while retaining all 53 tests.
  Combined, shared fixtures removed another 657 lines (2.2%) with no reduction
  in test count, bringing the suite to 29,705 lines and 598 tests.
- Full root discovery passed on CPython 3.10.13 with 545 tests and four
  environment-gated skips in 15.1 seconds. All 53 SDK tests passed on CPython
  3.14.3 in 0.6 seconds. Root and SDK Ruff lint and diff whitespace validation
  also passed. No dependency synchronization was performed.

Residual coordination caveat: the root workflow pins the currently published
SDK commit `96409ab`, and the compatibility pipeline passes against it. After
the SDK changes in this remediation pass are committed and published, update
that pin so the root workflow also exercises the new renewal/version tests.

Residual S3 consistency caveat: execution now performs a mandatory fresh route
and object-store plan immediately before deletion, closing the reproduced
plan-then-reference race. Because route publication and remote-object deletion
span different systems, a route can still change after that fresh read. A
strict guarantee needs a durable publication/deletion fence or equivalent
cross-system protocol rather than another example-based test.

Python 3.13 also reports existing `ResourceWarning`s when long-lived store
objects are garbage-collected with SQLite connections still open. The suite is
green, but explicit store lifecycle/cleanup should replace reliance on process
teardown; the warnings should not simply be hidden.
