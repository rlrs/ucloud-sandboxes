# File operations under node memory pressure

A Verifiers file upload failed with a plain HTTP 503 containing `direct node
memory pressure blocks active admission`. The file helper uses synchronous
`DirectSandboxService.exec`, which checks memory admission before dispatch.
Unlike the asynchronous exec endpoint, the file endpoint did not distinguish
this safe pre-execution rejection from a failure after a write had begun. The
SDK correctly refused to replay an unclassified PUT failure.

The fix marks capacity failures only around wake and exec admission, before the
command runner is called. File reads and writes absorb these failures within a
five-second wait window (also bounded by the configured admission wait). They
release the sandbox lifecycle lock and hold no exec reservation between
attempts. The original sandbox generation is checked on every attempt, and
drain or replacement stops the operation. Existing upload/read buffer admission
continues to bound retained payloads.

If pressure persists beyond that window, the node returns the existing
`node_active_exec_deferred` contract with `retryable: true` and `Retry-After`.
SDK 0.4.23 already retries this pre-dispatch fence for PUT and GET within the
caller's deadline. No SDK or Verifiers update is required for that version.
Errors from the command runner are never relabeled or retried by this wait.
The memory safety check remains in place; removing it would admit more work
while the kernel is already stalling on reclaim.

Tests cover transient memory PSI for reads and writes, dispatch exactly once,
bounded HTTP rejection with no dispatch, replacement and drain while waiting,
and an error after dispatch that must not be replayed. At the initial production
inspection there were no sandbox routes or pending creates. That idle snapshot
does not establish the cause or extent of memory pressure during the failed run.

## Release qualification

Runtime commit `87dc5a54b2c05247992d4090f0bbcf8d3e6ed060` was pushed and
deployed as 0.5.58 at 07:53 UTC. The installer verified 91 package files against
the wheel and selected validated 0.5.58 bundles for sandbox and builder nodes.

- Canonical checks passed: 933 server tests (six platform skips), 118 SDK tests,
  lint, wheel installation and Go tests. CI run 35498026702 passed.
- 243 targeted Linux tests passed (one platform skip).
- Both synchronous and asynchronous SDK 0.4.23 clients completed file upload
  and download against a real local node HTTP server with injected memory PSI.
  The upload outlasted the node wait window and retried successfully; the
  command runner received the write exactly once.
- Production worker 12396812 reported 0.5.58. Sixteen concurrent sandboxes
  passed three park/publish/resume cycles with persistent file hashes, binary
  upload/download equality, process identity and monotonic progress checks.
  All synthetic sandboxes were deleted; no routes remained.
- All 88 gateway health probes succeeded (p95 37 ms). Wake plus tool checks
  had p95 between 3.1 and 3.7 seconds. Cold creation took 71.8 seconds, including
  waiting for the replacement worker and image warmup; internal create retries
  were 460 `no_ready_node` and 12 `image_warmup_pending` responses.

The production canary verifies the deployed lifecycle and file paths; it did
not deliberately exhaust physical memory. Pressure recovery and replay safety
were exercised with controlled fault injection. This release does not establish
new 256- or 512-sandbox capacity results, and sustained pressure can still
exhaust the caller's request deadline.
