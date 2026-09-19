# Storage and resident-work admission, September 19

Production 0.5.47 separates metadata and snapshot publication from local storage
lifecycle admission. Version 0.5.48 also allows commands in resident sandboxes
to proceed under CPU contention. These changes address the measured production
queue amplification described in
[the latency diagnosis](prod-publication-bottleneck-2026-09-19.md).

## Changes

- Metadata reads (including the inventory used by heartbeats) no longer acquire
  the general storage operation semaphore. The journal still provides read
  consistency.
- Publication no longer holds a general operation slot while queued behind the
  publication budget or uploading to the registry. Publication concurrency,
  ownership fencing, and lifecycle transition checks remain in force.
- A wake blocked by sampled resource pressure refreshes the source heartbeat
  before selecting remote placement and demanding a portable snapshot. The
  existing refresh coalescing and timeouts remain in place.
- Existing sandbox commands, including file reads and writes, do not reject at
  a sampled CPU threshold. CPU remains shared through the sandbox cgroup.
  Missing metrics, memory safety, drain fencing, and generation checks still
  apply; creation and restore retain their separate admission protections.

No concurrency ceilings were raised for these changes. Physical disk, memory,
device capacity and bounded mutation/upload work remain necessary safeguards.

## Validation

The 0.5.48 canonical check passed 916 server tests (six platform-dependent
skips), 118 SDK tests, lint, shell checks, Go tests, and installed-wheel checks.
CI run 35438506552 passed. Regression coverage includes blocked publication
alongside metadata reads and another volume's mount, live source-pressure
refresh, resident file/tool operations under CPU saturation, and drain/owner
changes during metric collection.

Deployment commits: `c0f3ce8` (storage/source refresh, 0.5.47) and `28d45d2`
(resident CPU admission, 0.5.48). SDK 0.4.23 remains compatible; no SDK or
Verifiers update is required for these server changes.

### 64 large resident agents

[Raw qualification evidence](../benchmarks/admission-large-64-2026-09-19.json).
Run `admission-large-64-b9fc7722` used 64 concurrently created sandboxes,
512 MiB of random resident memory and a 128 MiB writable-disk file per agent,
on two workers running 0.5.48. The released asynchronous SDK 0.4.23 used a
separate client per sandbox, matching Verifiers.

All 64 initialized and completed two barrier-controlled relay cycles. Each
cycle held all model responses until all 64 agents were parked, then released
them together. The test verified resident-memory and file hashes, stable agent
identity, and tool execution after both restores. All 128 response commits
succeeded with exactly one commit attempt per response. Full-burst wake
delivery took 8.105 s and 6.596 s. There were zero migrations and zero snapshot
upload bytes in the sampled worker metrics.

Creation completed in 10.554 s (p95 9.673 s). Random-memory/file initialization
took 37.222 s; this includes generating 32 GiB of random memory and polling
readiness. All 101 gateway and 101 relay health probes passed; p95 latency was
30 ms and 14 ms. Cleanup left no qualification sandboxes or reservation.

SDK transport retries still occurred for bounded startup admission. This test
does not claim zero HTTP 503 responses: explicit busy replies were handled by
the SDK, and readiness polling saw expected file-not-yet-created responses.
The result demonstrates completed application operations without the earlier
CPU-pressure failures or publication/migration amplification.

The first attempt revealed two separate issues: CPU admission rejected two
reads on already running sandboxes (fixed in 0.5.48), and the test incorrectly
put its 128 MiB file on small `/tmp` storage. The final test writes the file to
`/workspace` and reports agent crashes directly. Only the corrected successful
run is counted above.

The subsequent upload-isolation test observed four active and four waiting
publications, while another sandbox on the same worker restored in 1.443 s.
A heartbeat immediately afterward took 22.5 ms. At the user's request the
remaining qualification was interrupted and its resources were cleaned up;
this is a partial observation, not a completed publication qualification.
The planned 512-way test was not launched.
