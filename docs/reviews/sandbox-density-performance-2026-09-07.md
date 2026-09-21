# Sandbox density and performance review — 2026-09-07

The isolated **32-vCPU / 96-GiB configured RAM** test node has completed
**128 persistent sandboxes through three correct park/wake cycles**, with
32 GiB of populated application memory and no operation or cleanup errors.
**The latency envelope is not yet qualified:** successful runs still miss the
three-second wake p95 and/or the 15-second full-wave target. Park latency now
usually meets the two-second p95 target after fixing gVisor references that
kept retired disks busy. Admission, storage allocation, checkpoint locking,
firewall reconciliation and lifecycle concurrency fixes are implemented and
tested; detailed results and failed trials are retained below.

Four unexpected VM poweroffs interrupted separate wake bursts. The user
identified these as a known UCloud platform bug; the consolidated
[incident log](ucloud-platform-incidents-2026-09-08.md) records provider times,
impact and evidence for later reporting. Interrupted runs remain failed.
The latest **16-worker v10 repeat completed all three cycles without errors or
a restart**, with park p95 **2.238 / 1.177 / 1.222 seconds** and wake through
work p95 **2.699 / 3.497 / 2.742 seconds**. Full wake waves took **15.942 /
15.993 / 16.202 seconds**, exceeding the 15-second target in every cycle;
queued completion p95 also exceeded ten seconds. Cleanup verified no remaining
owned sandboxes. The preceding 24-worker run suffered the fourth poweroff.

The user accepted these measured results as the release baseline on September
8 and authorized deployment. The original strict benchmark status and all
failed/interrupted results remain unchanged; acceptance is an operational
decision, not a claim that the initial SLO or all-family qualification passed.

Compatibility with the article's environments is a separate qualification.
The [Prime compatibility review](prime-compatibility-2026-09-07.md) inventories
all 23 families, records historical task results, and identifies remaining
network, data, image, grading and resource requirements. A density pass on the
fixture described here cannot establish compatibility with every task.

## The workload and resource envelope

The proposed baseline has 128 persistent Python processes, each configured
with a 1-vCPU limit, 1-GiB memory limit and 2-GiB writable workspace. Each
process retains **256 MiB of independently randomized memory**, an open Unix
socket, an open SQLite connection, a process nonce and a counter. An activity
step verifies the previous memory and persisted state, changes a declared
rotating set of pages, writes and checks 64 small files, commits SQLite state
and performs 100 ms of additional CPU work. The default stress profile dirties
every memory page; the separately identified hot-set profile dirties 16 MiB
while still retaining and hashing all 256 MiB.

This produces 32 GiB of deliberately populated guest memory across the node,
before sentry, filesystem-cache, Python and host overhead. The aggregate memory
limits are 128 GiB; they are ceilings rather than proof that all guests can
fill those limits together. The 128 configured CPU limits also exceed the
32 physical vCPUs. The benchmark defaults to 32 concurrent activity/lifecycle
workers and eight concurrent creates, reflecting bursts of work separated by
idle periods; the second live attempt used 16 activity/lifecycle workers.
It does not promise dedicated CPU throughput for 128 continuously busy
processes. The agent was configured for 96 GiB, while Linux reported 89,898 MiB
(about 87.8 GiB) of host memory. The initial runs reported no host swap;
after the reboot the node reported 98,303 MiB of swap. Every raw report
retains its own memory/swap baseline, so these states must not be conflated.

Parkable sandbox admission reserves more disk than the guest workspace. The
shared quota calculation includes writable disk, application-memory backing
rounded to the next 1-GiB allocator chunk, another complete guest-memory bound
for private checkpoint pages, and 64 MiB of metadata allowance.

| Profile | Per-sandbox workspace / memory limit | Per-sandbox hard disk claim | Claim for 128 sandboxes |
| --- | --- | --- | --- |
| Density fixture | 2 GiB / 1 GiB | 2,048 + 2,048 + 1,024 + 64 = **5,184 MiB** | **648 GiB** |
| Common upstream SWE defaults | 10 GiB / 4 GiB | 10,240 + 5,120 + 4,096 + 64 = **19,520 MiB** | **2,440 GiB** |

The configured pool exposes **1,449,984 MiB (1,416 GiB)** of hard storage after
subtracting Docker, swap, storage-cache and host headroom allocations. The
648-GiB fixture claim fits that shape. The upstream 2,440-GiB claim does not,
even before considering its 512 requested CPUs and 512 GiB of memory limits.
Sparse files do not remove these hard quota reservations. Smaller limits must
be explicitly resolved and recorded; the compatibility review describes the
upstream resource-override trap that can otherwise replace requested limits.

## Findings and changes

### Checkpoint publication blocked unrelated park and wake work

The artifact store held one root-wide exclusive lock while flushing all
checkpoint files, publishing their durable completion marker, and deleting
consumed generations. A paused memory-file `fsync` therefore blocked another
sandbox's wake cleanup and another park's directory preparation. The original
implementation failed the regression after a two-second wait for unrelated
cleanup.

The store now holds an exclusive lock per sandbox incarnation. All hibernation
generations belonging to that incarnation remain mutually exclusive, while
independent sandboxes can progress. Shared admission through the original root
lock preserves exclusion against older processes using the former exclusive
lock. File identity validation, required flushes, completion-marker ordering
and rollback fencing remain in force. Concurrent initialization of the store
directory and lock files is also covered.

The new lock files are retained after sandbox deletion to prevent waiters from
locking a replaced inode. Repeated park/wake cycles reuse them, but many new
incarnations on a long-lived node accumulate zero-byte inodes and directory
metadata. No hot path scans the root. Future artifact-lock maintenance can
acquire the original root lock exclusively before removal, which excludes all
incarnation lock holders and waiters; ordinary unlocked removal is unsafe.

### OCI swap configuration disabled its intended allowance

The old OCI config set `memory.swap` equal to `memory.limit`. OCI expresses
the combined RAM-plus-swap ceiling, and the pinned gVisor implementation
subtracts RAM when configuring cgroup-v2 swap. The old values therefore
disabled swap. The builder now uses twice the memory limit, permitting one
additional memory bound of swap. See the
[pinned conversion](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/runsc/cgroup/cgroup_v2.go#L837).

This resource correction does not require changing the checkpoint fingerprint
or draining nodes solely for format compatibility: pinned gVisor expressly
[allows resource-limit changes at restore](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/runsc/specutils/restore.go#L444).
Existing local bundles keep their prior setting; newly created or imported
bundles receive the corrected setting. Enabling swap does not itself establish
acceptable performance under reclaim or increase physical RAM.

### Free swap could conceal depleted physical memory

Admission previously considered available RAM plus free swap, allowing a node
with very little physical headroom to accept more work before the ten-second
memory-pressure average reflected reclaim. Create, wake and exec admission now
require at least **2 GiB of available physical memory** when physical memory
metrics are known. The existing combined-memory requirement still applies to
larger requested memory limits. CPU, load, PSI and hard-storage checks remain
additional conditions; admission tests do not demonstrate workload throughput.

### Load average rejected work without corroborating CPU saturation

The first 128-sandbox attempt failed during its initial activity burst: 13
execs received HTTP 503 with `direct node CPU load blocks active admission`.
The old gate rejected a one-minute load average of at least 1.25 times the
CPU count, independently of measured CPU utilization. On this node that
threshold is 40. Linux load average includes runnable tasks and
uninterruptible sleepers, and decays after those tasks finish; see the
[kernel implementation](https://github.com/torvalds/linux/blob/master/kernel/sched/loadavg.c#L16).
It is therefore insufficient evidence of current CPU saturation during
storage waits.

The load gate now additionally requires current CPU utilization of at least
**80%**. The separate **90% utilization** guard is unchanged. If the current
CPU measurement is unavailable, a high load average still blocks admission.
Tests cover the utilization boundary, unknown measurements, and both exec
and resource-bearing admission. This removes a demonstrated rejection path;
it does not fix slow storage or excuse latency failures. The successful
activity calls in that failed run already exceeded the two-second p95 target.

### The warm ublk pool does not subtract from the active-device limit

The default node-service ceiling is **128 active runtime owners plus pending
allocations**. Idle pool devices are reusable and are excluded from that
count. AgentEnv maintains one global idle pool, with a default high watermark
of 16, and Docker base-image layers do not allocate ublk devices. A regression
verifies 128 active owners alongside 16 idle devices and rejects the next
active allocation before journaling it. Retired devices retain their active
ownership until kernel teardown completes.

The second attempt began with **zero active backend owners**, 16 idle devices,
zero active creates and zero hard storage reservations, but one of its 127
additional creates failed with `storage-native ublk device capacity is
exhausted`. Together with the initial baseline sandbox, only 127 were created.
This occurrence was not caused by a leftover active owner from the first run.
The allocation reservation remained charged after the backend exposed its
new owner, until filesystem formatting and mounting finished. Concurrent
creates therefore counted the same device once as an owner and once as a
pending allocation.

The allocation code now transfers the reservation to backend ownership while
holding the device-slot lock, immediately after successful acquisition.
Slow format and mount work retain the owner charge without a second pending
charge. A deterministic two-slot regression blocks the first format: the
second create must succeed, while a third must be rejected before journaling.
It failed under the former accounting and passes with the fix. Failure tests
also cover acquisition errors, a lost response after ownership is established,
and rollback failure: reservations unwind, while retained owners continue to
consume capacity until release.

A separate kernel ceiling can matter: older kernels such as Linux 6.8 apply
`ublk_drv.ublks_max` to all devices, whereas newer kernels apply it only to
unprivileged devices. A global ceiling must accommodate both active and idle
devices: at least 144 for this configuration, with 256 providing additional
headroom. Pinned AgentEnv retries failed background pool refills without
backoff, making exhaustion of a global kernel ceiling a potential source of
allocation and log churn. The live node runs **Linux 7.0.0-30-generic**, and
the first run's resident snapshot records **144 live kernel devices: 128
active and 16 idle**. This verifies that the older 128-total-device ceiling
did not prevent density on this host. Details and kernel-source references are in the
[storage documentation](../object-storage-snapshots.md).

### Memory backing and durable workspace I/O share a storage path

The fixture deliberately populates and hashes all 256 MiB per process, then
changes every page on each activity step. The runtime places application
memory and the writable root in the same per-sandbox storage volume. In the
pinned AgentEnv backend, a
[ublk flush](https://github.com/kvcache-ai/AgentENV/blob/db1492b7915a408b37f863c9e3a34b2ccb2fb1b0/storage/ublk/src/impls/overlaybd_target.rs#L363)
syncs the image, which ultimately calls
[local file sync](https://github.com/kvcache-ai/AgentENV/blob/db1492b7915a408b37f863c9e3a34b2ccb2fb1b0/storage/overlaybd/src/backend/local.rs#L540).
SQLite durability can consequently wait for dirty memory-backing writes
already submitted to that same backing file. This is a source-supported
coupling mechanism; phase-specific tracing is still needed to assign its
share of the measured activity latency.

Read-only inspection during first-run cleanup at 21:50:28 UTC found CPU
utilization around 1.9%, but I/O PSI over the preceding minute was about
17.2% for some stalls and 13.1% for full stalls. The virtual disk's cumulative
average write completion was about 31 ms, including setup, creation and
cleanup. Those observations support investigating storage contention; they
are not synchronized measurements of the failed calls or an isolated disk
benchmark. Removing durability flushes would change the correctness contract.

The first resident snapshot reports 84,588 MiB of `MemAvailable` despite
32 GiB of populated guest memory. That value includes reclaimable file-backed
cache and must **not** be interpreted as evidence that the guests use only
about 5 GiB of physical RAM. Per-cgroup memory and process mapping/residency
measurements are needed to distinguish resident memory, backing-file cache
and reclaim. This run does not establish performance with all 128 guests
using their full 1-GiB limits.

### Restore startup removed per-sandbox CPU bounds

The Warden previously passed `--cpu-startup-burst` for every restore. The
pinned runtime removes the OCI CPU quota before boot, then derives its Go
scheduler CPU count from that temporarily unlimited cgroup. Eight restore
slots could therefore initialize eight schedulers sized for the whole
32-vCPU node. The candidate keeps the OCI quota throughout restore and leaves
the existing pressure threshold and restore-slot limit intact. The live attestation below confirms the effective cgroup quota; latency and
correctness still require a complete density run.

The candidate also moves exec capacity/activity cleanup outside the global
session lock, under a lock for the completing session. Terminal state is
published after cleanup attempts, and lifecycle coordination is released even
if the registry read fails. A deterministic blocked-cleanup test verifies that
another sandbox can read events and finish while the first cleanup is stalled.
Live sampling of the bounded restore candidate observed two sentry logical CPUs
and an effective one-CPU cgroup quota throughout sampled restores. A temporary
exec diagnostic found a 2.499-second process wait with stream joins below
0.06 ms and completion cleanup around 1.4 ms; the cleanup fix removes a
verified coupling but does not explain that live outlier.

## Local verification and measured scope

The Python candidate including CPU retries and retired-device reclamation passed the **824-test repository Python suite**
(five platform-dependent skips), **82 SDK tests**, managed-process Go tests,
lint, package builds and installed-wheel checks. The native Linux ACL helper
also passed on the isolated worker; the patched gVisor build and original
ACL oracle are separate gates. The benchmark includes 29 focused regressions
covering input/profile validation, event pagination and terminal races,
complete cleanup, late creates, failed deletion, retained storage owners
and missing storage metrics. Simulated tests are not live density evidence.

The [artifact-lock benchmark](../benchmarks/hibernation-artifact-lock-concurrency-2026-09-07.json)
published 128 tiny three-file generations using 16 workers and an injected
20-ms memory-file flush delay. Across three repetitions, median total time
fell from **3.369 seconds** with the former exclusive root lock to
**0.239 seconds** with incarnation locks. Other flushes used the real local
filesystem. This measures removal of a software queue; the files contain only
a few bytes and the delay is artificial. It measures neither real dirty-memory
bandwidth nor gVisor restore, ublk, remote storage or CPU fairness.

Historical small-sandbox observations are available in the
[August park/wake evidence](../benchmarks/hetzner-park-wake-optimization-2026-08-13.json):
park median about 0.25 seconds, attached wake median about 0.56 seconds, and
warm detached wake around 1.54 seconds. One empty-cache detached wake took
8.29 seconds. Those samples are useful context and do not supply density
percentiles for this candidate.

## Live acceptance procedure and current status

[benchmark_sandbox_density.py](../../scripts/benchmark_sandbox_density.py)
requires an idle dedicated direct node, verifies the reported 32-vCPU shape
and hibernation capability, and creates uniquely named test sandboxes. Its
default image is pinned by digest. It performs three park/wake cycles with
32 concurrent lifecycle workers and eight concurrent creates. Wake timing
includes the subsequent activity step, checking useful work after restoration
rather than only return from the wake API. The nonce, PID, resident-byte count,
counter, memory hashes, persisted files and SQLite state detect replacement
processes or lost/corrupted state.

The script requires the exact unique sandbox inventory, every sandbox running,
a complete heartbeat with the requested active count and zero in-flight creates
after creation and every wake wave. The script records host snapshots, workload and resource configuration,
individual timing samples, errors and cleanup evidence. It does not silently
retry admission failures. It distinguishes invocation duration from completion
time measured from the start of the whole phase, so executor queueing remains
visible. Exec completion uses the node's existing event condition long-poll,
draining through the terminal event, instead of opening a polling connection
every 20 ms. Each successful probe records available node exec-start timings;
the guest records elapsed time for memory verification, persisted-state
verification, memory dirtying/hash, file writes/reads, SQLite commit and CPU
work. These measurements do not change the workload or acceptance limits.

Cleanup uses at most eight concurrent generation-fenced deletes and drains
all submitted actions before checking for late creates. Both initial readiness
and final cleanup require storage-native nodes to report zero active ublk
owners and zero error volumes, and zero hard reservation when that metric is
present. Idle pool devices are allowed. Missing required storage metrics cannot
establish quiescence, and the cleanup deadline remains bounded.

A separate local, deterministic check found that exec completion holds the
global session mutex while releasing its activity lease. The direct lifecycle
release reads the registration database. An injected 200-ms stall in this
release blocked session and event reads for another sandbox until released.
This establishes cross-sandbox coupling; it does not establish actual database
latency or its contribution to the live benchmark. The candidate now releases these leases outside the global mutex, as
described above; terminal state remains fenced by completion of cleanup attempts.

The defaults below are **acceptance targets, not measured results**:

| Metric | Target |
| --- | --- |
| Park invocation p95 | ≤ 2 seconds |
| Wake through verified useful work, invocation p95 | ≤ 3 seconds |
| Activity invocation p95 | ≤ 2 seconds |
| Completion p95 from phase start | ≤ 10 seconds |
| Entire activity, park or wake phase | ≤ 15 seconds |

The isolated VM is UCloud job **12383398**, under the separate identity
**density-20260907**. The evidence below records agent version 0.5.31,
initialization version 3 and deployment identity `density-20260907`.

| Live attempt, UTC | Result | Measured scope |
| --- | --- | --- |
| [Two-sandbox smoke, 21:46–21:47](../benchmarks/sandbox-density-smoke-2026-09-07.json) | Passed two park/wake cycles and cleanup | Park invocation p95 ≤ 0.426 s and wake through verified work p95 ≤ 1.022 s; only two observations per phase |
| [128 sandboxes, concurrency 32, 21:47–21:51](../benchmarks/sandbox-density-128-c32-before-load-fix-2026-09-07.json) | Created all 128; failed initial activity before park/wake | Additional 127 creates took 83.884 s. Activity: 115 successes, 13 CPU-load admission failures; successful-call invocation p95 3.717 s, maximum 10.699 s |
| [128 requested, concurrency 16, 21:53–21:56](../benchmarks/sandbox-density-128-c16-before-allocation-fix-2026-09-07.json) | Created 127; failed creation before density activity or park/wake | One of 127 additional creates failed at the storage-device gate; the create phase took 79.693 s |

Both failed attempts report no cleanup errors or remaining owned IDs. Their
final snapshots show zero active sandboxes, zero active creates, zero active
backend devices and zero hard storage reservations, with 16 idle devices.
The second run's baseline confirms the same idle state. The first run's
successful-call percentiles exclude its failed requests and cannot constitute
a passing phase; they also independently miss the activity latency target.
Neither failed run provides a 128-sandbox park or wake measurement.

**Final patched 128-sandbox qualification: pending.** A repeat run must verify
the installed candidate containing both admission fixes, create all 128,
complete the configured activity and park/wake cycles within every latency
target, preserve all process/memory/file/SQLite state, and return the node
to its idle backend inventory. No live 128-sandbox performance pass is claimed
here.

The final live evidence must be added before treating the proposed envelope
as qualified. A successful shared-image fixture run would still leave cold
heterogeneous images, compiler/browser/test workloads, larger dirty sets and
remote detach/migration to be measured separately, alongside the real
environment graders described in the compatibility review.

### Third live run: allocations fixed; mass wake still fails

[Candidate v3, 128 sandboxes and 16 workers](../benchmarks/sandbox-density-128-c16-before-bounded-restore-2026-09-07.json)
created all 128 sandboxes, completed all 128 activity calls and parked all 128.
Activity p95 was **2.561 s** and park p95 **3.068 s**, with phase durations
**13.842 s** and **14.066 s**. These exceed the original service/completion
acceptance targets. Wake then had **36 admission failures** caused by the
90% CPU guard; its 92 successful requests have a censored p95 of **5.044 s**.
Those successful-request timings are not a full-population wake result.
Cleanup completed with no owned sandboxes, active devices or storage errors.

Guest profiling changes the interpretation of the activity bottleneck:
`memory_dirty_and_hash` p95 was approximately **1.865 s**, while full-memory
verification p95 was **0.211 s**, file writes/reads **0.070 s**, and SQLite
commit approximately **0.001 s**. Shared backing storage can couple durability
work, but SQLite commit is **not** the measured dominant cost in this run.
The fixture dirties every one of its 256 MiB of memory pages per action.
The benchmark now also supports an explicit smaller rotating dirty subset,
while still randomly populating and hashing all resident memory. The default
remains the full-dirty stress case; future subset results must identify both
resident and dirtied sizes separately.

One-second host CPU samples peaked at approximately 77% over this run, whereas
the admission sampler uses a 50-ms interval coalesced for 200 ms. A rejection
therefore proves a short saturation sample, not sustained whole-node CPU
saturation throughout the wake phase. The bounded restore candidate reduces startup fanout; its later measurements
are recorded below.

### Bounded restore candidate: full-dirty stress and a smaller hot set

The candidate wheel SHA-256 is
`0ee33b653f7a9cf165d482c89689d20a9e9b9ed150ef8defd86815c4be6d78f3`.
It includes the allocation, admission, completion-lock and bounded-restore
changes. These runs use the existing August runtime with the first hibernation
patch; the new default-ACL patch is not included.

| Run | Initial activity p95 / phase | Park p95 / phase | Wake outcome |
| --- | --- | --- | --- |
| [128, 16 workers, 256 MiB dirty](../benchmarks/sandbox-density-128-c16-full-memory-v4-2026-09-07.json) | 3.732 s / 16.907 s | 2.819 s / 13.116 s | 125 successes; three memory-PSI admission failures; successful-request p95 5.580 s |
| [128, 16 workers, 16 MiB dirty](../benchmarks/sandbox-density-128-c16-hot-memory-reboot-2026-09-07.json) | 0.774 s / 4.834 s | 3.223 s / 14.023 s | VM restarted during first wake phase; 28 successes, 100 failed requests; incomplete cleanup |

Both profiles retain and verify 256 MiB per sandbox (32 GiB across the node).
The smaller profile rotates a 16-MiB dirty subset and hashes every resident
byte, preserving corruption detection outside the recently modified pages.
Its activity improvement is a workload distinction, not a like-for-like speedup.
The full-dirty run completed cleanup with zero owned sandboxes and zero active
backend devices. Neither run is a density qualification pass.

A [two-sandbox diagnostic run](../benchmarks/sandbox-density-smoke-v4-diagnostics-2026-09-07.json)
completed three correct park/wake cycles and cleanup, but failed the original
activity p95 target due to a roughly 2.56-second outlier. It does not substitute
for a complete 128-sandbox run.

UCloud recorded the hot-set VM as suspended (powered off) at approximately
22:23:57 UTC and running again at 22:24:32 UTC. The previous boot journal ends
during XFS/ublk activity without a recorded OOM, panic or orderly shutdown.
The cause is undetermined. Following the reboot, a storage volume in ERROR
caused startup reconciliation to abort, preventing the node API from serving
requests. Recovery and final cleanup subsequently completed as described below; that run's
successful wake latencies must not be used as a full-population result.

### Recovery and eight-sandbox diagnostic

The recovery candidate quarantines terminal storage errors without attempting
to remount the broken volume. It fences the matching owned process, preserves
registration and storage ownership for explicit deletion, and allows the node
to serve healthy sandboxes. Repeated startup preserves quarantine; wake and
exec remain denied. Unknown fencing failures still fail closed.

On the rebooted worker, startup succeeded with 40 storage-error volumes.
[Generation-fenced cleanup](sandbox-density-reboot-cleanup-2026-09-07.json)
then removed all 128 owned test sandboxes and verified zero active creates,
active storage devices, error volumes and hard reservations. The original
interrupted run remains failed. See the [reboot evidence](sandbox-density-reboot-2026-09-07.md).

A fresh [eight-sandbox, two-cycle diagnostic](../benchmarks/sandbox-density-8-v5-diagnostics-2026-09-07.json)
passed all original targets and state checks, then cleaned up completely.
Each sandbox retained 256 MiB with a 16-MiB rotating dirty subset. Park p95
was **0.367 / 0.321 seconds**, and wake through useful work p95 was
**1.198 / 1.221 seconds**. [Temporary phase timing](../benchmarks/sandbox-density-8-v5-phase-timings-2026-09-07.json)
showed storage release as the largest park component in this small run.
This does not replace the missing complete 128-sandbox qualification.
The recovery node wheel SHA is `3d336446aa3066f771c02c0bb57202873fad6b610ea7af2a044ff375d74774b0`;
its storage service remains on the functionally identical storage code in v4.

### Built ACL runtime candidate

The separate [gVisor ACL build](gvisor-acl-build-2026-09-07.json) passed all
five test targets and produced a complete optimized distribution. The
[isolated installation](gvisor-acl-installation-2026-09-07.json) uses fresh
direct-runtime state; it does not reuse older checkpoints. The archive is
retained locally under `dist/gvisor-acl-20260907/` with SHA-256
`e27d1a86c89837ac06aa08ca2cb4236b161fa564b479ea92b1143c0dec1bb986`.
No production rollout is part of this review.

### First 128-sandbox ACL runtime trial

The [ACL runtime trial with 16 workers](../benchmarks/sandbox-density-128-acl-c16-hot-2026-09-07.json)
created all 128 sandboxes and completed all initial activity and park calls.
Each sandbox retained 256 MiB and dirtied a rotating 16-MiB subset. Activity
p95 was **0.775 seconds** with a **4.988-second** phase; park p95 was
**3.118 seconds** with a **14.606-second** phase. Park completion p95 was
**14.567 seconds**, also over its target. Seven wake calls failed the existing
CPU-pressure check. The 121 successful wake calls had a censored p95 of
**2.766 seconds**; the whole wake phase took **14.670 seconds**. Cleanup
returned active sandboxes, creates, backend devices and storage errors to zero.

The [synchronized analysis](../benchmarks/sandbox-density-128-acl-c16-analysis-2026-09-07.json)
retains the distinction between successful-request latency and phase success.
Park's storage-release span dominated at **2.483 seconds p95**; checkpoint
capture was **0.069 seconds**, artifact commit **0.145 seconds**, and stopping
the runtime **0.119 seconds**. The storage span includes both overlay teardown
and the storage service's durable release, which requires finer tracing before
assigning the cost to a specific operation.

Thirteen complete one-second host intervals during wake averaged **39.1% CPU
execution**, with a maximum of **63.9%**, and about **5.2% I/O wait**. These
intervals do not resolve the admission sampler's 50-ms windows. The seven
failures cluster near 10.5–11 seconds into wake; bounded resampling can be
tested without changing the pressure threshold. Eliminating those failures
alone would not meet the park and fleet-completion targets.

One complete sample verified 128 distinct sandbox cgroups, each still limited
to one CPU. Their aggregate `memory.current` was **35.92 GiB**. File-backed
charges were **33.58 GiB**, including **33.27 GiB** mapped files, with
**1.90 GiB** of anonymous memory. The sentry boot processes' PSS is only a
partial process/mapping view and must not be substituted for total guest
memory. The raw [host samples](../benchmarks/sandbox-density-128-acl-c16-host-2026-09-07.jsonl.gz)
and [timing journal](../benchmarks/sandbox-density-128-acl-c16-journal-2026-09-07.jsonl.gz)
are retained with the benchmark.

### Lower concurrency and bounded CPU admission

The [eight-worker repeat](../benchmarks/sandbox-density-128-acl-c8-deep-2026-09-07.json)
again created all 128 sandboxes. Activity p95 was **0.631 seconds**, and park
p95 improved to **1.397 seconds**. Parking the full population still took
**14.795 seconds**, and completion p95 was **13.498 seconds**. Eleven wake
requests failed CPU admission; the 117 successful calls had a censored p95
of **1.732 seconds**, while the wave took **18.333 seconds**. All owned
sandboxes and storage devices were cleaned up. The
[host analysis](../benchmarks/sandbox-density-128-acl-c8-analysis-2026-09-07.md)
records partial creation coverage and again distinguishes one-second CPU
utilization from the shorter admission samples.

The next candidate adds a **one-second CPU admission retry deadline** to
create, wake and exec. It waits approximately 210 ms between samples so the
200-ms production cache can expire. Sampling and waiting happen outside the
capacity lock. The 90% utilization threshold and corroborated 80% load
threshold remain unchanged; missing metrics and simultaneous memory failures
are not retried. Drain interrupts the wait, and shape, ownership and admission
fences are rechecked before work begins. A late sample cannot grant admission
after the retry deadline. A hung custom metrics provider is outside this
deadline; it bounds retries rather than forcibly cancelling provider code.
The node-only wheel SHA is
`8bc834925cdf8f8760003c7414eb7388ea49d1690897663817df89c094c7ad0d`.

The [three-cycle v6 trial](../benchmarks/sandbox-density-128-acl-v6-c16-wait-2026-09-07.json)
completed all 128 wake-and-state checks in each of the first two cycles,
without CPU admission errors. It still failed latency limits, and the third
wake wave admitted only 24 sandboxes: **104 requests hit the active-device
capacity limit** while retired backend owners remained charged. Cleanup
subsequently removed every owned sandbox, active device and storage error.

| Cycle | Activity p95 | Park p95 / phase | Wake p95 / phase | Wake successes |
| --- | --- | --- | --- | --- |
| 0 | 0.795 s | 2.984 s / 14.397 s | 3.079 s / 16.515 s | 128/128 |
| 1 | 0.812 s | 2.364 s / 13.125 s | 2.442 s / 14.075 s | 128/128 |
| 2 | 0.797 s | 2.305 s / 13.934 s | 2.259 s / 7.291 s, censored | 24/128 |

The raw [host samples](../benchmarks/sandbox-density-128-acl-v6-c16-host-2026-09-07.jsonl.gz)
and [node/storage timing records](../benchmarks/sandbox-density-128-acl-v6-c16-journal-2026-09-07.jsonl.gz)
are retained. Deep storage tracing found that the kernel's exclusive-open
probe repeatedly returned `EBUSY` for the full 500-ms release wait; the service
then durably retired the device instead of exposing it to another owner.
In the preceding eight-worker run, 124 of 128 parks took that retirement path.
The device remains counted until a later exclusive-open check and exact-owner
check permit reclamation. Neither kernel reuse fencing nor durability should
be weakened to conceal this cost.

A separate three-sandbox control paused for 45 seconds after parking the first
sandbox while leaving two others running. The first device was still busy
15.7 seconds into that hold, and was reclaimed by a later observation after
the hold and subsequent parks. Process mountinfo, open-file and mapping scans
did not identify a visible reference on that device. This does not prove a
specific stale descriptor or rule out detached mount trees. Its deliberately
injected pause is diagnostic evidence, not a latency benchmark.

### v7: three complete cycles; latency still misses acceptance

The [v7 run](../benchmarks/sandbox-density-128-acl-v7-c16-reclaim-2026-09-07.json)
created all 128 sandboxes and completed all 384 park and 384 wake-through-work
operations, preserving the memory, process and filesystem checks. No operation
errors occurred, and cleanup verified zero owned sandboxes or active devices.
This establishes repeated lifecycle correctness for the stated 256-MiB resident,
16-MiB rotating dirty-set fixture on this 32-vCPU node. It does not establish
the latency targets or compatibility with heterogeneous upstream tasks.

| Cycle | Activity p95 | Park p95 / phase | Wake through work p95 / phase |
| --- | --- | --- | --- |
| 0 | 0.802 s | 3.065 s / 14.068 s | 2.834 s / 15.065 s |
| 1 | 0.782 s | 2.327 s / 14.322 s | 2.809 s / 14.260 s |
| 2 | 0.795 s | 2.302 s / 13.616 s | 3.257 s / 15.497 s |

The v7 wheel SHA-256 is
`0223ff7c68cbdf8abd14a29c9a8b036a0ef0a4ea3b738d70b5028057d4380a9b`.
On device-capacity pressure, admission now coordinates bounded reclamation
outside the allocation lock, then rechecks backend ownership plus pending
reservations under that lock. Only the exact retired owner with a successful
exclusive-open check can be reclaimed. Busy devices remain charged. The
two-second bound covers retries and retirement-lock waits; backend calls keep
their existing command timeout. The existing 500-ms park release wait is
unchanged. [Full repository verification](../benchmarks/sandbox-density-final-check-2026-09-08.json)
and its complete log are retained.

### Runtime mount retention fix (2026-09-08)

A subsequent namespace control identified detached mount references in later
sentries: their executable mappings and Go's cached `cpu.max` descriptors
referenced cloned host/cgroup mounts with namespace ID zero. Ordinary mountinfo
hid these references. The [statmount-by-FD evidence](../benchmarks/gvisor-detached-mounts-before-2026-09-08.json)
and [namespace observations](../benchmarks/gvisor-detached-mounts-before-2026-09-08.jsonl.gz)
are retained. An earlier [exclusive-open probe](../benchmarks/gvisor-busy-device-before-2026-09-08.json)
returned `EBUSY` despite finding no ordinary namespace reference.

Patch `0003-ucloud-release-detached-mounts.patch` now opens the sentry executable
in the parent host namespace, uses its donated descriptor for both direct and
prewarmed execution, and closes the descriptor before boot re-execution. Sentry
and gofer children disable Go's automatic cgroup CPU watcher. The sentry uses
two scheduler processors during bootstrap, then retains gVisor's explicit CPU
sizing. OCI quotas and block-device exclusive-open fencing remain enforced.
The pinned build passed all seven configured test gates and the optimized
complete release build. The [installation manifest](gvisor-mountfix-installation-2026-09-08.json)
identifies the exact binary and companions installed on the idle dev node in
a fresh runtime generation. No production runtime has been changed.

The first three-sandbox control completed all state checks and cleanup. During
the repeated control, two later sentries used the original host executable mount
(namespace ID 8), had no `cpu.max` descriptors, and retained only two active
storage devices after the first sandbox parked. The injected hold makes these
controls unsuitable for latency acceptance. The [58 hold samples](../benchmarks/gvisor-mountfix-control-analysis-2026-09-08.json)
confirm this state from 1.26 to 58.70 seconds after park, with zero retired
devices throughout. The original [ACL oracle](prime-acl-mountfix-2026-09-08.json)
passes all 9 tests with reward 1 and cleanup verified. The [ten-cycle hibernation repeat](gvisor-mountfix-hibernation-2026-09-08.json)
also passes all existing checks. The first [128-sandbox repeat](../benchmarks/sandbox-density-128-mountfix-c16-2026-09-08.json)
was interrupted by a VM poweroff during its second wake wave. All 128 first
wakes and both park waves completed before the interruption. The first park
p95 was **2.489 seconds**, its full wave **10.318 seconds**; the second park
p95 was **1.129 seconds**, its wave **7.986 seconds**. First-wake p95 was
**2.933 seconds** and its wave **15.864 seconds**. These still miss some
unchanged latency thresholds. Only 56 second-wake calls completed, so their
latencies are censored and must not be presented as a successful density run.

UCloud reported the VM powered off at **06:12:58.143 UTC** and running again
at **06:13:41.261 UTC**. The retained host telemetry has 136 valid one-second
samples ending at **06:12:53.447 UTC**, plus a trailing 1,124-byte zero-filled
record after the abrupt restart. Guest `oom_kill` remained zero and swap-out
counters did not rise during the captured run; this does not establish the
cause of poweroff. The retained kernel records have no panic/OOM/shutdown
message. The pre-reboot [host telemetry](../benchmarks/sandbox-density-128-mountfix-c16-host-2026-09-08.jsonl.gz),
[kernel journal](../benchmarks/sandbox-density-128-mountfix-c16-kernel-2026-09-08.jsonl.gz)
and [service journal](../benchmarks/sandbox-density-128-mountfix-c16-services-2026-09-08.jsonl.gz)
are preserved without replacing the invalid trailing data.

A test-only storage diagnostic entrypoint was in `/tmp` and disappeared on
reboot, delaying API recovery. It has been moved to `/var/tmp/density-diagnostics`
and the test wrapper updated. Product package code was unaffected by this
harness repair. Original benchmark cleanup failed while the API was unavailable;
a separately recorded, [generation-fenced cleanup](sandbox-density-mountfix-reboot-cleanup-2026-09-08.json)
then completed with zero remaining sandboxes, active devices, storage errors
or hard reservations. The 24-worker repeat completed as recorded below.


The [captured first-wave storage trace](../benchmarks/gvisor-mountfix-storage-release-analysis-2026-09-08.json)
contains 128 release operations and **zero exclusive-open failures**. Backend
release p95 is **45.4 ms**, replacing the old 500-ms busy-device wait. Remaining
storage tails include journal transition p95 **94.4 ms** and completion p95
**243.8 ms**, with maxima of **1.55** and **2.42 seconds**. Those observations
identify journal contention as a remaining latency contributor; no journal
transaction/durability changes have been made based only on this trace.


### 24 request workers, eight restore slots

The [three-cycle repeat](../benchmarks/sandbox-density-128-mountfix-c24-2026-09-08.json)
completed all 128 sandboxes in every phase with zero operation errors or
cleanup errors. All park and activity thresholds pass. Wake remains outside
the unchanged targets. Host telemetry and the detailed service journal are
retained beside the raw benchmark.

| Cycle | Activity p95 | Park p95 / phase | Wake through work p95 / phase |
| --- | --- | --- | --- |
| 0 | 0.909 s | 1.901 s / 8.111 s | 4.688 s / 16.189 s |
| 1 | 0.944 s | 1.703 s / 9.545 s | 3.201 s / 13.144 s |
| 2 | 0.910 s | 1.730 s / 8.259 s | 3.684 s / 14.186 s |

The [wake-stage analysis](../benchmarks/sandbox-density-128-mountfix-c24-wake-analysis-2026-09-08.json)
shows restore-slot waiting p95 **2.062 seconds**, network reconciliation p95
**473 ms**, and Warden resume p95 **1.259 seconds**. These overlapping/component
quantiles must not be added. The next controlled repeat raises the existing
`max_concurrent_restores` setting from 8 to 16 on the same idle 32-vCPU node;
request concurrency stays 24. CPU/memory admission thresholds, guest quotas,
state verification and latency acceptance remain unchanged. This is a
configuration trial, not a change to the general deployment default.


### Restore-limit trial and firewall verification

The [16-restore-slot trial](../benchmarks/sandbox-density-128-mountfix-c24-r16-2026-09-08.json)
also preserved all 128 sandboxes through three cycles and cleaned up fully,
but worsened wake p95 to **6.118 / 4.558 / 4.272 seconds**, with waves of
**16.931 / 15.441 / 15.222 seconds**. Park and activity still passed. The test
node has been reverted to eight restore slots; sixteen is not recommended
from this evidence.

Candidate v8 reads the current IPv4 firewall rules once per host reconciliation
using `iptables-save`. An exact rule match avoids a separate `iptables -C`
process; absent rules and failed, malformed or incomplete reads retain the
original check-and-repair path. The snapshot is local to that invocation,
never cached across wakes. DNS-backed egress refresh, kernel lease validation,
private-network denials and exact-port policy are preserved.

The [real Linux qualification](../benchmarks/network-rule-snapshot-qualification-2026-09-08.json)
runs in a separate network namespace. All 11 required checks matched; deleting
a private-network deny triggered repair, and a different TCP port could not
stand in for the required rule. Across 25 alternating samples, median host-rule
reconciliation fell from **14.03 ms to 2.50 ms** (p95 **15.43 ms to 2.68 ms**).
This microbenchmark does not establish end-to-end wake latency. The
[qualification source](../benchmarks/network-rule-snapshot-qualification-2026-09-08.py)
is retained. All 121 direct-runtime tests passed, including four new firewall
snapshot regressions.

The node-only v8 wheel SHA256 is
`9af4a6e9215b90f4ad24e64f8998e1ded59969fc28737f5874697ac75d7e1188`.
Storage remains on the v7 wheel, and the gVisor mountfix distribution is
unchanged. The 24-request/eight-restore-slot repeat is recorded below.


### v8 result and journal writer coordination

The [v8 density run](../benchmarks/sandbox-density-128-mountfix-v8-c24-2026-09-08.json)
completed all 128 sandboxes across three cycles, with no operation or cleanup
errors. It still fails latency acceptance: park p95 is **2.154 / 1.721 / 1.689
seconds**, wake through work p95 **3.879 / 3.503 / 3.875 seconds**, and wake
waves **15.810 / 14.793 / 16.784 seconds**. The firewall microbenchmark alone
was insufficient to predict end-to-end improvement.

Candidate v9 coordinates short local SQLite writer transactions with a mutex.
Readers retain independent connections. SQLite's own locks still fence other
processes; `BEGIN IMMEDIATE`, revisions, operation replay and FULL synchronous
commit semantics are unchanged. An unfinished transaction is rolled back
before handing the writer slot to another request. No backend work, guest
execution, or kernel block-device fencing is moved inside the writer guard.

The [Linux microbenchmark](../benchmarks/journal-writer-qualification-2026-09-08.json)
runs 512 durable counter transactions with 16 workers, alternating old and new
coordination over three fresh journals. Every committed increment is verified.
Old wave durations are **443 / 541 / 748 ms**, versus **368 / 391 / 363 ms**;
maximum request waits fall from **432 / 534 / 742 ms** to **22 / 34 / 14 ms**.
Median requests become slower because writers share access more evenly; one
old run's p95 also beats the coordinated p95 while retaining a 742-ms outlier.
The source and complete samples are retained; this is not a sandbox SLO pass.

All 28 storage-daemon tests pass, including a new concurrent writer-failure,
rollback/handoff and independent-reader regression. The v9 node/storage wheel
SHA256 is `6ba6d749684928c1c37219270fa9f92b16f8ba88bdd1dffacb47cdbc9a45d045`.
The same 24-request/eight-restore-slot density profile is being rerun.


### v9 platform interruption; v10 network lease locking

The [v9 run](../benchmarks/sandbox-density-128-mountfix-v9-c24-2026-09-08.json)
completed two correct cycles and both first wake waves for all 128 sandboxes.
Its third wake was interrupted by another UCloud VM poweroff: 95 requests
completed and 33 timed out. Service recovery and the benchmark's own cleanup
completed with no cleanup errors. This run remains failed. The user identified
the poweroffs as a known UCloud platform bug and requested incident records
for later reporting; [observed incidents](ucloud-platform-incidents-2026-09-08.md)
are logged with provider timestamps and guest evidence. Platform root-cause
investigation is no longer part of this performance task.

The first two v9 wake p95 values were **4.550 / 3.742 seconds**, with waves
**15.458 / 15.326 seconds**. Park p95 was **1.784 / 1.680 / 1.750 seconds**.
The local writer microbenchmark did not by itself establish the full wake SLO.

The network manager previously held one node-wide lease lock throughout
namespace/veth setup, including every host and namespace command. Candidate
v10 keeps slot allocation and shared firewall verification under that global
lock, while a persistent per-incarnation file lock protects namespace setup
and deletion. Independent sandboxes can now configure their kernel networking
concurrently. Deletion leaves the slot allocated until kernel cleanup has
finished, then rechecks the lease before returning the slot. Lock inodes are
retained to avoid splitting an existing waiter's lock from later callers.

The new regression uses two manager instances on the same state directory:
a stalled namespace setup allows another sandbox to finish; deletion of the
stalled incarnation waits, and its slot is not reused before cleanup. Existing
migration, exact-egress, durable allocation and repair tests still pass.
[Full verification](../benchmarks/sandbox-density-v10-check-2026-09-08.json)
passes 830 main tests (5 skipped), 82 SDK tests, lint, Go and package checks.
The [unchanged ACL oracle](prime-acl-v10-2026-09-08.json) passes 9/9 with reward
1 and its sandbox deleted, exercising actual package downloads and grader
execution through the candidate's network path.

The [node installation](../benchmarks/sandbox-density-v10-installation-2026-09-08.json)
uses wheel SHA256
`ce083a63597eea4ef5fbe14cca9c37a9d6ca61a79a658c42611cd017de5bc059`.
The storage daemon keeps the identical storage code in the v9 wheel, with
SHA256 `6ba6d749684928c1c37219270fa9f92b16f8ba88bdd1dffacb47cdbc9a45d045`.
The gVisor distribution and eight-restore-slot limit are unchanged. The [24-request-worker repeat](../benchmarks/sandbox-density-128-mountfix-v10-c24-2026-09-08.json)
completed two wake waves, but was interrupted during its third by platform
incident 004. Its first two park p95 values were **1.794 / 1.731 seconds**;
wake p95 was **5.397 / 4.283 seconds**, with waves **16.930 / 16.146 seconds**.
The third wake had 104 successful calls and 24 timeouts; its quantiles are
censored and do not establish latency. Cleanup completed with no errors.
The change has not established an end-to-end speedup. The completed 16-request-worker repeat below uses the same candidate, eight
restore slots and unchanged SLOs.


### Latest complete run: v10, 16 request workers, eight restore slots

[Raw result](../benchmarks/sandbox-density-128-mountfix-v10-c16-2026-09-08.json),
[host samples](../benchmarks/sandbox-density-128-mountfix-v10-c16-host-2026-09-08.jsonl.gz),
and [service journal](../benchmarks/sandbox-density-128-mountfix-v10-c16-services-2026-09-08.jsonl.gz)
retain the complete benchmark interval. All 128 sandboxes passed state, memory,
filesystem, SQLite and process checks after each wake. There were no operation
or cleanup errors, no remaining owned sandboxes, and no VM restart. The result
is **failed on latency**, not a qualification pass.

| Cycle | Activity p95 | Park p95 / full wave | Wake through work p95 / full wave | Wake queued-completion p95 |
| --- | --- | --- | --- | --- |
| 0 | 0.763 s | 2.238 / 9.120 s | 2.699 / 15.942 s | 15.457 s |
| 1 | 0.768 s | 1.177 / 8.115 s | 3.497 / 15.993 s | 15.265 s |
| 2 | 0.805 s | 1.222 / 8.178 s | 2.742 / 16.202 s | 15.440 s |

Request concurrency reduced service latency compared with the 24-worker v10
run, but did not improve whole-wave throughput enough to meet the target.
The [preceding diagnostic trace](../benchmarks/sandbox-density-128-mountfix-v10-c24-wake-analysis-2026-09-08.json)
shows network setup p95 around 235 ms; remaining time includes restore-slot
waiting and storage remount/journal work. The Warden stage named
`validate_artifact` includes storage mount, so its duration must not be
attributed solely to checkpoint verification. These interrupted-run stage
samples identify follow-up profiling targets; they are not a latency pass.

The candidate remains installed only on the dev node. Production gateway
runtime configuration has not been changed. Compatibility certification for
all 23 environment families and the full 128-sandbox latency acceptance remain
outstanding; the evidence does not justify claiming either is complete.
