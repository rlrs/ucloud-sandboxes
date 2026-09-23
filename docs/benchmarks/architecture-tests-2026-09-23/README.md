# Linux architecture regression qualification

The server suite ran on rasmus-dev against a real PostgreSQL database and the
released SDK 0.4.26, using the locked server dependencies. Native runtime and
production performance gates are recorded separately; these tests do not establish
loaded latency.

- `full-linux.log`: 1,504 tests, 12 expected skips, 127.648 s. Before the public
  workspace contract and extracted lifecycle commit service.
- `rc9-full-linux.log`: 1,513 tests, 12 expected skips, 124.422 s. Includes those
  boundaries and restore admission using real serialized v2/v3 checkpoint metadata.

The restore regression replaces a misleading mock whose file fields did not match
the real artifact manifest. It publishes and reloads the actual metadata and runs
service wake admission against both persisted layouts.

- `rc9-python310-postgres-socket.log`: 129 tests passed together in 23.778 s on
  Python 3.10, including real PostgreSQL, HTTP relay, stateful recovery, socket
  handoff and lifecycle commits. An initial quiet invocation stalled after 39
  tests and was stopped; the exact combined order passed on a bounded verbose
  rerun with scheduled traceback dumps. The stall was not reproduced.
- `sqlite-workload-linux.log`: 22 benchmark tests passed in 2.217 s. Includes the
  optional FULL-synchronous SQLite WAL workload, retained reader/writer state,
  recovery from DB+WAL without shm, and corruption rejection. Native park/restore
  and loaded latency remain separate required gates.

- `rc10-full-linux.log`: exact frozen rc10 package, 1,529 tests passed in 188.386 s
  against real PostgreSQL and SDK 0.4.26; 12 expected environment skips. Includes
  wake admission/incarnation fences, owner-scoped storage operation identities,
  bounded restore demand waiting, cumulative CPU accounting and SQLite workload.
  The package intentionally retains rc9 `environment_rootfs.py` and
  `environment_backend.py`; their two test files also retain rc9 versions. The
  separate, disabled P4 single-component image fix is not covered by this gate.

- `rc11-full-linux.log`: exact frozen rc11 package with current P4 modules and
  nested legacy heartbeat decoding, **1,531 tests passed in 189.357 s**, 12
  expected environment skips, against real PostgreSQL and SDK 0.4.26. All test
  files are current; there are no P4 exclusions in this gate.

- `post-rc11-startup-identity-linux.log`: 143 tests passed in 50.050 s for the
  later startup preflight and placement reread incarnation fences. Covers actual
  health/fleet HTTP with legacy nested metrics, rejection before bind, real
  delete/recreate races, gateway lifecycle, capacity and consolidation. These
  follow-up changes are separate from the frozen rc11 full-suite result.

Post-rc11 latency fixes: `cpu-sampler-linux.log` records 146 passing Linux
resource/admission/provisioner/node tests (41.6s). CPU intervals now come from
existing background collection while physical memory/PSI remain foreground
reads. `gateway-enqueue-linux.log` records 119 passing gateway/transport tests
(48.3s); `gateway-enqueue-cancel-linux.log` adds the final focused cancellation
case, 18 passing tests (1.0s). Cross-thread loop wakeups no longer hold the shared
transport guard; shutdown waits for accepted enqueue reservations before stop.
These are component/regression evidence, not a new production latency claim.

- `rc12-full-linux.log`: exact frozen rc12 package, **1,554 tests passed in
  190.923 s**, 12 expected environment skips, real PostgreSQL and SDK 0.4.26.
  Includes current P4 retry, CPU background sampling, enqueue shutdown fencing,
  startup durable-state readiness, bounded small uploads and workspace formatting.
  There are no exclusions.
- `rc12-python310-postgres-socket.log`: the combined real PostgreSQL/relay/socket
  order passed **133 tests in 21.398 s** on Python 3.10, with a bounded run and
  delayed traceback capture enabled. No timeout or thread hang occurred.

- `wake-placement-linux.log`: **219 Linux tests passed in 56.847 s** for the
  post-rc12 canonical wake use case, actual durable migration/route fences,
  gateway translation and existing async response integration. Direct domain
  tests require no HTTP handler. This is a focused assembled-source gate, not
  the frozen rc12 full-suite result.

- `rc13-full-linux.log`: exact frozen rc13 package code, **1,579 tests passed in
  197.099 s**, 12 expected environment skips, real PostgreSQL and SDK 0.4.26.
  Includes canonical P2 wake placement and stale-publication races, async small
  file uploads, keyed relay delivery, worker exec phase timing and unconditional
  first-transition physical-headroom admission. There are no source exclusions.
  The release changes only version metadata from 0.5.113 to 0.5.114rc13 after
  this snapshot; the tested package code is identical. This closes the assembled
  P2 regression gate, not the outstanding loaded latency or density gates.
- `rc13-python310-postgres-socket.log`: the exact rc13 mirror also passed the
  combined Python 3.10 PostgreSQL/relay/socket order: **140 tests in 26.984 s**.
  The run was bounded at 180 seconds with delayed traceback capture enabled;
  there was no timeout or thread hang.
- `resident-backing-linux.log`: post-rc13 backing-aware resident policy and
  runtime integration, **46 Linux tests in 0.545 s**. The isolated mirror uses
  rc13 plus the policy, capacity types, metric decoder and constructor binding;
  the separate in-progress growth-admission changes are excluded. This is a
  focused policy gate, not an assembled rc14 or SIGBUS remediation claim.
- `rc14-full-linux.log`: frozen rc14 ran **1,611 tests in 199.752 s** with
  **8 fixture errors** and 12 expected skips. Every error came from the same
  managed-control fixture, whose minimal registry stub lacked the now-required
  durable growth inventory. The fixture now uses `DirectSandboxRegistry`; all
  eight affected cases pass. This failed gate is retained, not counted as a
  passing release qualification. rc15 also includes two independent review fixes
  for unknown resource evidence and imported-primary terminal observations.
- `rc15-full-linux.log`: exact frozen rc15 package, **1,613 Linux tests passed
  in 200.041 s**, 12 expected environment skips, real PostgreSQL and SDK 0.4.26,
  without source exclusions. Includes RAM-backing capacity and resident policy,
  durable managed growth forecasts, unknown-resource admission, imported-primary
  terminal release, required exec admission and equivalent guest-path parsing.
  Candidate metadata is already 0.5.114rc15; no post-gate version override was
  needed. Pressure correctness and sustained workload checks remain separate.
- `rc15-python310-postgres-socket.log`: unchanged rc15 mirror passed **165 tests
  in 27.493 s** on Python 3.10, combining real PostgreSQL/relay/socket behavior
  with exec admission, managed growth and backing-aware resident policy. The
  bounded run had no timeout or thread hang.
- `managed-start-retry-linux.log`: **17 Linux tests passed in 5.416 s** for
  the post-rc15 managed-start admission correction. Real worker HTTP and released
  SDK 0.4.26 sync/async clients each receive a pre-dispatch 503, retain the same
  queued primary identity and retry successfully when headroom is released.
  An ambiguous supervisor failure is neither reclassified nor automatically
  replayed. No SDK upgrade is required for this existing retry contract.
- `rc16-full-linux.log`: exact frozen rc16 package passed **1,621 Linux tests
  in 205.759 s**, 12 expected skips, real PostgreSQL and SDK 0.4.26, without
  exclusions. Includes the narrow managed-start and continuation pre-dispatch
  admission classification and the crossed-capture/full-growth restore regression.
- `rc16-python310-admission.log`: **48 tests passed in 10.875 s** for the changed
  admission paths on Python 3.10, including real HTTP SDK sync/async retries,
  ambiguous-dispatch preservation, continuation, growth and worker handlers.
- `managed-read-http-linux.log`: post-rc16 read-handler correction, **24 Linux
  tests passed in 10.756 s**. The status/log handlers now catch typed temporary
  read unavailability before its semantic-error parent. Actual HTTP tests prove
  released SDK 0.4.26 sync/async retries for both reads and unchanged non-retryable
  409 responses for genuine semantic conflicts. Start/signal semantics are unchanged.
- `rc17-full-linux.log`: exact frozen rc17 package passed **1,625 Linux tests
  in 211.037 s**, 12 expected skips, real PostgreSQL and SDK 0.4.26, without
  exclusions. It includes the actual HTTP managed-read retry regressions.
- `rc17-python310-managed-reads.log`: the changed read-handler and admission
  paths passed **10 tests in 6.460 s** on Python 3.10, including both SDK clients.

- `rc18-full-linux.log`: exact frozen rc18 package passed **1,635 Linux tests
  in 212.733 s**, 12 expected skips, real PostgreSQL and SDK 0.4.26, without
  exclusions. The completed result was recovered after the task interruption;
  the suite was not rerun merely because the interactive session stopped.
  Includes worker poll/respond pre-BEGIN pool admission, unchanged ambiguous
  model-request behavior, unknown-footprint capture serialization and rejection
  of memory samples taken before the current safe wait.
- `rc18-python310-postgres-resident.log`: unchanged rc18 mirror passed **117
  tests in 17.275 s** on Python 3.10, combining actual PostgreSQL HTTP behavior
  with runtime, resident-memory, ranking, reclaim and backing-policy tests.
  The first invocation named a nonexistent `test_warm_park` module; its 90 valid
  cases passed but the invocation failed import. That log is retained as
  `rc18-python310-invocation-error.log`; only the corrected run is a passing gate.
- `pressure-budget-linux.log`: **26 Linux harness tests passed in 2.464 s**
  after adding an explicit per-sandbox request budget. Default 180 seconds is
  unchanged; zero, negative and non-finite inputs are rejected. The named
  pressure-correctness profile raises only this budget to 1,800 seconds, retaining
  the overall workload deadline, full working set, concurrency and integrity gates.
