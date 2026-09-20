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
