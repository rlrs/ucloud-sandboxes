# Cold-start burst investigation, September 18

The latest `super-park-harness-*` traffic matches the reported 256-way failure
around **15:14–15:18 UTC (17:14–17:18 CEST)**. The earlier 16- and 64-way passes
were reported by the caller; this investigation did not repeat that harness.

## Worker loss is distinct from admission pressure

Worker **12396030** started at 13:16:01.672 UTC. UCloud reported it **SUSPENDED**,
with “The virtual machine is powered off,” at **15:15:34.327 UTC**. Its last
heartbeat at 15:15:23 reported 80 active sandboxes and 14 active creates. CPU was
27.23% and memory 9.28%; storage counters were all zero, including the maximum
ublk-device count, so that storage sample is not useful proof of free capacity.
The preceding sample showed 67 active / 83 live devices, maximum 128.

The retained autoscaler provider-operation journal contains one stop intent for
this worker, created **15:15:35.577 UTC**, *after* the provider poweroff report.
It records `post_start_suspension`, `postStartSuspensionObserved=true`, and 146
routes associated with the lost worker. UCloud then reported SUCCESS at
15:15:35.608. This establishes the ordering: our destructive-loss cleanup
followed the suspension. It does not establish who or what initially powered
off the VM. No expiry was present in the retained job record. The near-two-hour
runtime is not evidence of a two-hour limit.

Two upload traces began at 15:15:22.993 and 15:15:23.537, before the poweroff.
They spent 62.465 and 62.160 seconds waiting for node response headers and
returned 504. Their waits straddle worker loss; attributing their entire delay
to ordinary overload would be incorrect. The trace IDs are
`ecab7f888eef46be080a613dd296e8c1` and `376cf0d1bbe7c17214eee1cbf610781a`.
Other requests returned 503 or hit the five-second connection limit.
Owner-loss cleanup provides an explanation for missing routes; it cannot
reconstruct an unpublished running sandbox from lost local VM state.

## Why the burst concentrates on too little startup capacity

The harness requested 2 vCPU, 1024 MB RAM and 5184 MB hard disk per sandbox.
The worker advertises 32 vCPU, 96 GiB RAM and 1,449,984 MiB allocatable disk.
The current placement policy adds disk reservations, but multiplexes CPU and
RAM using measured pressure. That small disk shape fits roughly 279 times on
one worker under the disk calculation. This is an occupancy estimate, not a
promise that 256 cold starts or restores can run simultaneously. The node also
has separate storage-device and operation-concurrency limits.

At 15:15:04 the fleet still had one ready worker. A rolling window counted 2080
create rejections (retries, not unique sandboxes), but CPU was 68.45%, and there
were only two observations. Create-pressure scale-out requires the same
sustained host-pressure proof as ordinary scale-out. Thus the gateway's busy
signal did not independently provision burst headroom. A replacement/additional
worker, **12396120**, was submitted at 15:15:25 for a capacity deficit and first
became ready around 15:16:11.

A fixed create limit alone does not cover the entire startup sequence. File
uploads execute a command inside the sandbox and can trigger a restore. The
restore semaphore allowed eight operations but queued additional callers
without a deadline while holding a request thread and sandbox lifecycle lock.
Per-sandbox lock contention and downstream operations can still impose waits;
the patch below specifically removes the restore-slot queue.

## Confirmed amplification and the patch

A gateway lookup for one sandbox fetched the worker's entire inventory. For
each parked sandbox, the node handler synchronously fetched storage metadata
and potentially built its published snapshot descriptor. Retained inventory
traces show repeated `storage.client.GetVolume` calls and responses arriving
after the gateway's five-second read deadline. Concurrent polling repeated this
work across all parked sandboxes.

The patch adds a filtered internal inventory query backed by one registry
lookup and a lifecycle snapshot. Unfiltered inventory and filtered lookup use
the existing generation-keyed publication cache. Background publication/startup
hydration still fill that cache; the explicit descriptor endpoint retains its
storage validation. A new gateway remains compatible with older workers that
ignore the query and return their full list.

Restore-slot admission now fails before restore/network/tool work when all
slots are occupied. The node returns HTTP 503, `node_restore_busy`,
`retryable=true`, and `Retry-After: 1`. Explicit wake and implicit file/exec wake
share the same slot guard. Slots are released on completion and exceptions.
The SDK peer checkout adds this precise rejection to its existing bounded,
jittered safe-retry mechanism. Safe pre-dispatch sandbox rejections use the
caller deadline instead of an independent sixteen-attempt cutoff. It does not
retry ambiguous upload 504 responses.
Older SDKs must be upgraded to consume this new retry contract automatically.

## Validation and remaining work

Local regressions cover filtered lookups with live inspection/storage calls
made fatal, cached publication metadata, old-worker response compatibility,
restore rejection without side effects, successful subsequent retry, and the
HTTP retry contract. SDK tests exercise sync and async uploads through twenty
admission rejections while preserving the body, and refusal to replay an
ambiguous timeout. The full local server suite passed **876 tests, six skipped**;
the SDK suite passed **97 tests**. Ruff and diff whitespace checks passed.

## Further startup optimizations

The follow-up adds a shared node startup budget (eight by default) spanning
create, restore and file I/O. The deployment bootstrap passes the scheduler's
`create_target_concurrency_per_node` through to that budget. Upload admission
happens before buffering the body. Nested restore reuses an admitted file
request's slot; the restore-specific limit still applies. Interactive lifecycle
lock contention rejects before command execution rather than holding a request
thread indefinitely. Deletion and maintenance retain their lifecycle fences.

The gateway similarly shares its configured create budget across creates and
operations that can wake sandboxes. It rejects before body buffering and closes
the rejected connection. Health, heartbeat/status reads and deletion stay out
of this budget. The SDK recognizes `gateway_startup_busy` and `node_startup_busy`
as safe rejection fences, retains the caller deadline and jitter, and caps
backoff exponent computation for long queues.

A sustained durable capacity queue can now request bounded early headroom even
below the CPU threshold: at least eight pending requests by default, oldest
capacity request at least 30 seconds old, fresh observations and no idle ready
worker. Queue age is computed separately from warm-reservation age and excludes
suppressed non-capacity failures. Existing headroom/provisioning/fleet bounds
apply; a booting worker counts toward the target, preventing repeated scale-out.
No production policy setting was changed.

New local tests hold a cold create open while verifying restore/file rejection
and readable inventory. A 256-request admission simulation admits eight,
explicitly defers 248, and releases all permits after success/failure. Real local
HTTP tests reject an upload whose body has not arrived while continuing to serve
health and deletion. Policy regressions cover low-CPU backlog scale-out, fresh
versus stale/short demand, idle capacity, provisioning credit and reservation-age
isolation. These tests isolate overload handling; they are not 256 real gVisor
sandboxes and do not establish production throughput.

The initial VM poweroff remains unexplained by retained gateway telemetry.
These optimizations do not recover unpublished running state from a lost VM,
provide a durable server-side work queue, or bound every downstream storage and
process operation. Clients retain admission retries until their deadline. No new
256-way production test was run. These changes were subsequently deployed in
0.5.35; see the [deployment verification](release-0.5.35-deployment-2026-09-18.md).

Follow-up validation: the canonical `scripts/check.sh` completed successfully:
883 server tests (six skipped), 99 SDK tests with integration dependencies,
Ruff, shell syntax/ShellCheck, managed-process Go tests, package builds and
installed-wheel smoke checks. The five startup-scaling tests also passed after
adding the explicit non-default bootstrap-limit regression. ShellCheck was
installed into a temporary tool directory for this run. No runtime dependency
or production environment was changed by these checks.
