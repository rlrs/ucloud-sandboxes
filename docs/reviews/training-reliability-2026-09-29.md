# Training reliability and build deadlines — 2026-09-29

**A build that completes after the training job's deadline is a failure.** The
latest optimization substantially improves repeated builds, but it does not yet
qualify cold builds, dependency changes or a sustained build burst alongside
500 running agents. Every end-to-end deadline must pass. This review made no production changes.

The subsequent fixes, SDK release and production canary are recorded in the
[build reliability release](../benchmarks/build-reliability-2026-09-29/README.md).

## Confirmed bug: transient status timeout becomes permanent build failure

A [local proof](training-reliability-2026-09-29/build-owner-404-proof.json) exercises
real gateway methods and the published SDK without HTTP or production calls.
The owner first reports **200/running**. A simulated owner timeout becomes a
transport **504**, but the gateway converts it to **404**. The SDK treats 404 as
terminal and aborts after **one poll despite a 600-second budget**. Restoring the
owner yields **200/running** again: the build still exists.

The per-owner RPC deadline is **five seconds** (`control_plane.py:236`);
[lookup handling](../../ucloud_sandboxes/control_plane.py#L2933) loses the transient
failure while searching owners (lines 2933–2944, 2973–2981, 3001–3017). SDK
`client.py:930–934` treats 404 as terminal. Under a delayed builder this can fail
training even when the build succeeds. Preserve a transient 503/504 response and
bounded SDK polling retries; only confirmed absence should become terminal 404.
The reproduction establishes the bug, not its frequency in customer runs.

## Confirmed build-slot robustness gap

A [local reproduction](training-reliability-2026-09-29/build-deadline-proof.json)
used the exact SDK wait method and a bounded 1.5-second server child. A
**0.1-second SDK deadline raised `TimeoutError` while server status remained
`running` and one slot remained occupied**. The next build was rejected with
`ImageBuildCapacityError`; the slot returned only after normal child completion.
This proves client timeout does not cancel server work, not that production
currently has a hung build.

That asynchronous behavior needs an explicit capacity safeguard:
[`_run_streaming`](../../ucloud_sandboxes/images.py#L473) blocks on stdout reads
and `process.wait()` without an execution deadline. No outer build deadline or
cancel route bounds this work. A hung subprocess can therefore retain a slot
indefinitely; enough such builds exhaust admission. Server deadlines/cancellation
must terminate and reap owned work, persist its terminal outcome and release
capacity safely, while preserving deliberate detached builds and idempotent retry.

## Confirmed async SDK blocking and timeout budgets

The [SDK 0.4.33 proof](training-reliability-2026-09-29/sdk-archive-lag-proof.json)
ran four concurrent submissions of the same incompressible **16-MiB owned context**
with mocked HTTP: **4.019 seconds** elapsed, with a **4.009-second gap** in a
5-ms event-loop heartbeat. Source matches the published wheel; its SHA is in the
receipt. `client.py:2449–2456` calls synchronous tar/gzip/hash preparation
(`2813–2828`) before its first await. This can stall unrelated agents sharing
the runner. The 48-build benchmark uses a **synchronous thread pool**, so it does
not qualify async responsiveness. Archive work needs bounded offloading/reuse.

Submission defaults to **600 seconds** for archive/upload/admission. Explicit
`build_image(timeout_seconds=...)` budgets submission plus completion; omitted
completion timeout is unbounded. Inspect separately defaults to **1,800 seconds**.
These are not evidence of the actual training runner's budget. Synchronous
archive work also delays event-loop timeout handling.

## What is measured, and what remains unqualified

The [latest repeat](../benchmarks/build-cache-single-import-2026-09-29/README.md)
completed 48/48 builds on four fresh eight-CPU builders, with populated shared
registry caches and the same frozen contexts. Client p95 fell from **97.275 to
21.683 seconds**, and batch time from **98.594 to 54.656 seconds**. However,
client maximum remained **54.637 seconds**: case 22 selected a complete cache
but still executed a 19.9-second application build. The [outlier evidence](../benchmarks/build-cache-single-import-2026-09-29/case22-outlier.md)
shows overlapping graph work; the internal cache decision remains unproven.

Admission is now the dominant typical wait: latest submission p95 was
**19.522 seconds**, versus build/push p95 **2.355 seconds** and post-admission
worker queue p95 **0.006 seconds**. The four-builder pool exposes 16 execution
slots to a 48-request burst. The SDK retried 55 submit 503 responses. Phase
percentiles cannot be added, and a recovered retry is only acceptable if the
complete request still meets its deadline.

The earlier [six-phase qualification](../benchmarks/build-load-2026-09-29/README.md)
passed 138 builds, but cold/dependency client p95 was **162.8/128.7 seconds**.
These pre-fix measurements are not current predictions. The exact-cache repeat
does not requalify cold/dependency/replacement work or provisioning latency.

The shared cache retains [64 owned tags globally](../../ucloud_sandboxes/build_cache.py#L320),
preferentially by newest publication timestamp, not a per-context working-set
guarantee. The 48-context repeat fits that budget; **192 or 500 distinct contexts
do not**. Distinct builds can evict useful exact entries and force fallback/work
again. Test this churn before extrapolating warm-cache latency; it is a retention
policy tradeoff, not evidence of corrupted images.

The [current harness](../../scripts/live_build_load_benchmark.py#L146) permits
at most **96 requests/48 concurrency** and refuses existing sandboxes, so it
cannot directly qualify the requested mixed load. Its per-request clock starts
inside the executor task, excluding local queue delay for later waves. The
batch clock includes that delay, but request deadline statistics do not.

## Shared gateway and registry exposure

The last [whole-host agent acceptance](../benchmarks/gateway-capacity-2026-09-28/README.md#actual-ccx23-validation)
passed 512 real agents/6,144 cycles, with response-ready-to-usable p95 **0.740 s**.
All 512 were active for **222.70 seconds**. Synthetic bulk traffic overlapped for
**110.192 seconds** at 124.662 MiB/s NAT, 132.876 MiB/s distinct registry uploads
and 24.878 MiB/s warm reads. It was not simultaneous real builds, cold reads or
checkpoint pressure, and it predates the subsequent relay and builder changes.

Whole-host CPU was **2.347 mean / 3.169 p95 / 3.860 maximum cores** on four CPUs.
During mixed traffic, CPU PSI averaged 32.56%, I/O PSI 25.73%, and registry await
19.38 ms. No network/listen drops were observed; TLS averaged 0.119 cores.
Shared CPU/registry contention is the stronger measured concern. The earlier cold-build phase independently had
250.7 MiB/s p95 registry writes and **180.5 ms p95 disk request latency**. Neither
separate test proves the combined workload meets build deadlines; their CPU
percentiles must not be added to invent a mixed result.

At **15:00–15:02 UTC**, the four-CPU/16-GiB gateway was idle and healthy:
zero fleet nodes, four active control services with no restarts since deployment,
PostgreSQL up since September 28's planned resize, about 13.1 GiB free memory,
registry 16% used and all five APT units masked. No kernel OOM/I/O fault since
boot or gateway/relay/placement errors since deployment were observed. This is
idle health, not loaded proof.

## Required deadline qualification

1. Pin the actual training SDK/runner and **real caller deadline**. Start every
   request's clock before local queueing/context preparation; include provisioning,
   upload, admission/retries, build/push, publication and successful image use.
   Report each deadline miss and maximum, not just p95 or terminal success.
2. Exercise cold recipes, repeats, source/ARG/dependency changes, 192/500 distinct
   contexts, replacement after pruning, no-ready-builder startup, sustained
   arrivals and bursts. Preserve realistic recipe independence.
3. Run alongside 512 representative agents for **at least four hours**, then
   extend toward training duration. Include model-return bursts and checkpoint
   traffic; measure driver event-loop/socket pressure, slot occupancy, build
   phases, host CPU, disk/PSI and retention. Inject owned builder-status delays
   beyond five seconds and client deadline expiry; verify eventual slot recovery.
4. Require **every build within its declared deadline**, correct runnable images,
   no duplicate/lost requests, and preserved agent state/progress. Separate
   declared fault windows, retain the agent latency gate outside them, and prove
   bounded queues/resources. Prebuild/prewarm or bound admission when capacity
   cannot meet deadlines; simply increasing timeouts is not a fix.

## Additional memory and recovery boundaries

Two workers cannot retain 1,000 private 512-MiB heaps: **500 GiB** exceeds their
configured **360 GiB**, before overhead. The [growth policy](../managed-growth-admission.md)
accepts possible SIGBUS when a continuation exceeds its observed peak near full
RAM backing; a kernel OOM is not required. On worker loss,
[routing](../../ucloud_sandboxes/routing.py#L244) recovers only a parked route with
a complete published portable checkpoint; other non-deleting routes become
`TERMINAL_REPLACE`. These need separate pressure/recovery gates; the earlier
idle reboot did not test active training survival.
