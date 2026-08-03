# gVisor live-state parking plan

Status: pinned two-phase runtime, direct-runsc node Warden, image-only Docker
path, crash-safe local generations, and bounded wake coordinator implemented
and benchmarked, 2026-07-28; production serving integration remains gated.

## Decision

Parking must preserve the complete running process tree. A
filesystem-persistent but process-ephemeral restart mode is intentionally out
of scope because the service cannot reliably infer whether a workload depends
on background processes, open descriptors, sockets, interpreter state, or
shell-local state.

The target is an AgentENV-like live-state lifecycle on gVisor's `systrap`
platform, without KVM or nested virtualization:

```text
running -> parking -> parked -> restoring -> running
```

This does not change resource guarantees. Every admitted sandbox's writable
disk limit must remain physically backable, and the configured memory
overcommit must remain backed by RAM, swap, or a reserved hibernation file.
Parking may improve the amount of those resources that is resident and the
latency with which they are reused; it cannot turn sparse allocation into a
hard guarantee.

## Workload and capacity model

For `N` logical sandboxes, model-generation time `G`, and mean tool execution
time `E`:

```text
wake rate             = N / (G + E)
expected active count = N * E / (G + E)
```

At `N=1000`, `G=30s`, and `E=1s`, the node receives about 32.3 wakes per
second and has about 32.3 active sandboxes on average. This matches a 32-vCPU
node only as an average; synchronized RL batches and longer tools require a
bounded runnable-slot queue.

For an incremental parking implementation with `D` bytes dirtied and later
read per turn, a conservative full-cycle storage rate is:

```text
2 * D * wake_rate
```

At 32.3 wakes per second this is approximately 1.0 GiB/s for 16 MiB of dirty
state, 4.0 GiB/s for 64 MiB, and 16.1 GiB/s for 256 MiB. Consequently,
per-turn parking is viable only if measured dirty working sets are small or
restores avoid reading most captured pages.

## Phase 1: stock runsc background restore

Use uncompressed runsc checkpoints and restore with `--background`.

Stock gVisor separates kernel state, page metadata, and page contents for
uncompressed checkpoints. Background restore starts the workload after kernel
metadata is available, loads remaining memory and file pages asynchronously,
and prioritizes pages touched by the workload.

This phase improves time to first instruction but is not incremental:

- checkpoint still scans and saves all relevant non-zero application pages;
- total read volume remains approximately the checkpoint size;
- saving again waits for an earlier asynchronous page load to complete;
- a successful restore command only means background loading started;
- a late page-load failure can terminate an apparently ready sandbox.

The staged checkpoint must remain usable until runsc has opened its page
files. On POSIX storage it may then be unlinked: open descriptors keep the
blocks alive and charged until restore completes. Capacity accounting must use
filesystem free space rather than assume unlink immediately reclaimed bytes.

Benchmark foreground and background restore with identical checkpoints and
report:

- checkpoint duration and artifact allocated bytes;
- restore command duration;
- time to first successful `ls`;
- time to a memory-touch command over 16, 64, 256, and 1024 MiB;
- `runsc wait --restore` completion time;
- page-read bandwidth and peak host RSS;
- a second checkpoint immediately after first instruction and after full
  restore.

The second-checkpoint measurements are essential: they expose the cost hidden
behind a fast first instruction.

### Phase 1 benchmark, 2026-07-27

A same-host A/B benchmark ran on a 32-vCPU AMD EPYC 9535 UCloud VM with
96 GB advertised RAM, local XFS-backed Docker storage, and runsc
`release-20260721.0`. The workload held non-zero anonymous memory in a
checkpointed child process. Each cell is one correctness-checked live fork;
these are directional measurements, not latency SLOs.

`start` is the `docker start`/raw runsc restore duration.
`first tool` runs a lightweight `docker exec hostname -i` immediately after
start and includes one Docker network inspection. Warm samples restore
immediately after checkpointing. The cold sample calls `sync` and drops the
dedicated VM's host page cache after sealing the checkpoint.

| Saved anonymous memory | Cache | Foreground start | Background start | Change | Foreground first tool | Background first tool | Change |
| ---: | :---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 MiB | warm | 460 ms | 471 ms | 2.4% slower | 582 ms | 589 ms | 1.2% slower |
| 1 GiB | warm | 1,116 ms | 1,057 ms | 5.3% faster | 1,239 ms | 1,184 ms | 4.4% faster |
| 4 GiB | warm | 2,824 ms | 1,169 ms | 58.6% faster | 2,952 ms | 1,401 ms | 52.5% faster |
| 1 GiB | cold | 2,207 ms | 590 ms | 73.3% faster | 2,333 ms | 837 ms | 64.1% faster |

Checkpoint time scaled almost linearly and was unaffected by the restore flag:
about 3.6 seconds at 256 MiB, 14 seconds at 1 GiB, and 54–59 seconds at 4 GiB.
The raw samples are in
[`benchmarks/gvisor-background-restore-2026-07-27.json`](benchmarks/gvisor-background-restore-2026-07-27.json).

The result supports enabling `--background` for live forks: it is nearly free
for small warm checkpoints and materially improves large or cold restores.
It does **not** support checkpointing every sandbox on every model turn.
At this host's observed checkpoint rate, saving 1 GiB takes roughly 14 seconds
and saving 4 GiB roughly 56 seconds before sealing, staging, or restore.
Per-turn parking needs phase 2 or phase 3, or evidence that real dirty resident
sets are far smaller.

## Phase 2: soft parking under memory pressure

For short model-generation intervals, retain the runsc sandbox and pause its
workload. Let host memory pressure and swap evict inactive pages. This preserves
live process state and already provides page-granular writeback and
demand-paged wake-up.

Measure:

- irreducible sentry, gofer, thread, file-descriptor, and page-table overhead;
- resident and swapped bytes per idle sandbox;
- wake latency after 0%, 25%, 50%, 75%, and 100% of its resident memory has
  been reclaimed;
- aggregate tail latency under synchronized wake bursts;
- swap write amplification and interference with Docker's XFS quota device.

This phase can improve use of the existing 2x memory overcommit. It cannot
support a memory guarantee larger than the combined reserved RAM and swap
backing.

### Phase 2 benchmark, 2026-07-27

A stock-runsc benchmark used the same 32-vCPU UCloud host as phase 1, with
87 GiB usable RAM and 24 GiB swap. Parking was `docker pause` followed by a
cgroup-v2 `memory.reclaim`; waking was `docker unpause` followed by
`docker exec true`.

For a single non-zero working set, reclaim removed nearly all application
memory from RAM. A lightweight first exec stayed fast because it did not touch
the original mapping, while scanning the complete mapping exposed an effective
fault-in rate of about 120--124 MiB/s:

| Working set | Resident before | Resident parked | Swap parked | Reclaim | First exec | Full mapping touch |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 MiB | 286.3 MiB | 4.7 MiB | 282.2 MiB | 283 ms | 109 ms | 2.14 s |
| 1 GiB | 1,057.3 MiB | 6.9 MiB | 1,050.6 MiB | 1.02 s | 119 ms | 8.49 s |
| 4 GiB | 4,137.6 MiB | 13.1 MiB | 4,124.3 MiB | 4.39 s | 123 ms | 33.01 s |

At a density of 1,000 empty gVisor sandboxes, the aggregate cgroup working set
was 17.70 GiB resident. Pause alone left 17.67 GiB resident. Explicit reclaim
reduced this to 3.15 GiB resident plus 15.22 GiB swap: an 82.2% reduction in
RAM residency, but no reduction in the amount of memory that must remain
backed.

The matched 24-way wake test establishes the latency tradeoff:

| State of 1,000 sandboxes | 24-wake p50 | p95 | max | Burst makespan |
| :--- | ---: | ---: | ---: | ---: |
| paused, resident | 206 ms | 235 ms | 238 ms | 247 ms |
| paused, reclaimed | 2.93 s | 4.05 s | 4.08 s | 4.09 s |

Waking all 1,000 reclaimed sandboxes in 32-wide batches took 84.7 seconds;
individual p95 and p99 latencies were 9.17 and 17.02 seconds. Deleting the
remaining paused and swapped sandboxes also progressed slowly until they were
unpaused, so expiry and GC need a separate bounded path.

The result supports a two-temperature design without a custom runsc:

- **hot park:** pause while the model generates, retain resident memory, and
  resume on a tool call; this matched AgentEnv-like wake latency but does not
  increase memory density;
- **cold park:** reclaim older paused sandboxes to the node's existing swap;
  this frees RAM, but wake latency becomes workload- and burst-dependent;
- keep enough hot parked sandboxes for the expected inference completion
  burst, and admit cold wakes through a small page-in queue;
- use the RL coordinator's explicit generation/tool boundary rather than an
  idle-time heuristic, and unpause a cold sandbox before destructive GC.

`memory.reclaim` returned `EAGAIN` after partial progress in these runs, as the
cgroup-v2 interface permits. Production code must use a resident target,
measure the result, and retry or accept partial reclaim; one write is not a
transactional park operation. The raw measurements are in
[`benchmarks/gvisor-soft-parking-2026-07-27.json`](benchmarks/gvisor-soft-parking-2026-07-27.json).

### Swap fault-path investigation, 2026-07-27

The approximately 120 MiB/s full-touch result is not the virtual disk's
sequential bandwidth and is not primarily a gVisor limit. On a second,
identical UCloud compute product, direct `fio` measured:

| I/O pattern | Queue depth | Throughput | IOPS |
| :--- | ---: | ---: | ---: |
| 1 MiB sequential write | 32 | 1.77 GB/s | 1,689 |
| 1 MiB sequential read | 32 | 5.94 GB/s | 5,661 |
| 4 KiB random read | 1 | 20.9 MB/s | 5,093 |
| 4 KiB random read | 32 | 485 MB/s | 118,480 |

With the default `vm.page-cluster=3`, a sequential 1 GiB swap-in took
8.64 seconds natively, 7.94 seconds under runc, and 8.42 seconds under runsc.
Native, runc, and runsc therefore all delivered about 118--129 MiB/s. Each
major fault fetched an eight-page, 32 KiB cluster; roughly 33,000 disk reads
restored 1 GiB. The fault path behaved close to queue depth one and left most
of the virtual disk's available parallelism unused.

Increasing the host-wide swap cluster improved a sequential 256 MiB runsc scan
from 119 MiB/s at the default to 345 MiB/s at cluster 6 and 524 MiB/s at
cluster 8. It did not improve the representative scattered exec path. A sparse
1 GiB mapping with 1,024 touched pages took 165 ms at cluster 3 and 191 ms at
cluster 8. Linux adapted its readahead, so the larger setting caused little
extra I/O in that synthetic sparse case, but it was still slower.

The 1,000-sandbox experiment showed an additional first-use cache effect:

- resident pause/resume of 24 sandboxes took 191 ms on the debugging node;
- the first reclaimed 24-way exec batch at cluster 8 took 3.41 seconds;
- after the shared host exec/control path had been exercised, untouched
  reclaimed sandboxes took 718 ms at cluster 3 and 870 ms at cluster 8;
- repeating an exec on already-used sandboxes took 813 ms at cluster 3.

Thus the original four-second result combines per-sandbox swap faults with a
cold shared exec/control path. A final cache-control run made that effect
explicit. After dropping host file caches, one untouched reclaimed canary
sandbox took 154 ms and read 27.6 MB. The next 24 untouched reclaimed
sandboxes then completed in 253 ms total and read only 19.1 MB combined.
Serially priming one runsc exec path on the node avoided the cold shared-page
stampede without restoring every sandbox's working set.

A larger `page-cluster` accelerates complete, sequential working-set
restoration but is not a general fix for tool-call wake-up. Keep the default
swap cluster until representative traces show that eager, mostly sequential
restoration is common enough to justify a host-wide change. The low-risk
implementation experiment is instead a node-level runsc exec canary before the
node accepts bulk tool traffic, plus periodic measurement of whether its
shared path remains resident. Real first and repeated tool commands must still
be traced because commands that touch application state will pay their own
swap-in cost.

The raw attribution data are in
[`benchmarks/gvisor-swap-debug-2026-07-27.json`](benchmarks/gvisor-swap-debug-2026-07-27.json).

## Phase 3: custom disk-backed runsc

### Feasibility decision

This is realistic without nested virtualization, but it should be split at a
much earlier cut line than a full hibernation implementation.

The source review is pinned to gVisor `release-20260721.0`, commit
`9f653e577965df2ddd13875b5530cd2588661f1c`. It found:

- `pgalloc.MemoryFile` already accepts an `*os.File`, maps it `MAP_SHARED`, can
  punch holes when pages are released, and has explicit `DiskBackedFile` and
  `DecommitOnDestroy` options.
- runsc already uses disk-backed `MemoryFile`s for private tmpfs and overlay
  filestores in `runsc/boot/vfs.go`.
- only the main application `MemoryFile` is hard-coded to a memfd, in
  `runsc/boot/loader.go:createMemoryFile`.
- runsc already has an FD-donation path from the trusted parent through
  `runsc/sandbox/sandbox.go`, `runsc/cmd/boot.go`, and `boot.Args`.
- the existing sentry seccomp policy already permits `fsync(2)`,
  `madvise(2)`, `mincore(2)`, and `sync_file_range(2)`.

The first useful fork therefore does not need to invent a new pager or state
format. It can keep the sentry and gofer alive, make the main `MemoryFile`
disk-backed from sandbox creation, and explicitly page it out while the kernel
is paused:

```text
                       regular, private XFS file
                                  |
                                  v
application mappings <-> pgalloc.MemoryFile <-> host page cache

running --park--> paused + pageout --resume--> demand-paged running
```

This mode is called **disk park** below. It removes the per-park scan and second
copy of all application pages. Dirty pages are written to their canonical
backing by ordinary Linux writeback over the sandbox's lifetime; parking has to
write only pages that are still dirty, then reclaim clean pages. Waking is an
ordinary runsc resume followed by file-backed page faults.

Disk park retains the sentry, gofer, kernel object graph, descriptors, sockets,
and process state. The measured empty-sandbox footprint therefore remains a
real RAM cost. At the current roughly 18 MiB per live sandbox, 1,000 live
sentries are about 18 GiB before reclaim. Eliminating that fixed cost is a
separate, substantially more invasive **local hibernation** stage.

### Non-goals for the first implementation

- no filesystem-only stop/restart mode;
- no KVM, nested virtualization, or AgentEnv's ublk pager;
- no cross-node migration;
- no memory cloning or reflink-based fork;
- no claim that sparse files relax hard storage guarantees;
- no transparent change to already-running memfd-backed sandboxes.

### Resource guarantee

The canonical file allocates disk blocks for application pages even while
those pages are resident in RAM. A disk-backed sandbox must therefore reserve
for the worst case, not its current `st_blocks`:

```text
per-sandbox disk reservation =
    writable disk hard limit
  + application memory hard limit
  + bounded runtime/checkpoint overhead
```

The node must reject admission unless the sum of these reservations fits on
the XFS device after its safety margin. XFS project quota limits one sandbox;
a crash-durable node reservation ledger prevents the sum of project quotas
from exceeding the physical device. Current allocated blocks are an
observation and reclamation signal, never admission capacity.

This is more conservative than the current 2x RAM overcommit. The existing
design needs backing only for memory that cannot remain in RAM; canonical
file-backed memory reserves disk for the complete memory limit. With a 2 TiB
device, 1,000 sandboxes requesting 1 GiB of memory consume 1 TiB of reservation
before their writable-disk limits and safety margin. This tradeoff must be
included in every capacity comparison.

High logical density also requires a bounded active-tool queue. The backing
files prevent data loss and OOM when many sandboxes are cold, but cannot make
1,000 simultaneous working sets fit in RAM or make their page faults fast.

### Runtime patch 1: external backing FD

Add an opt-in `application-memory=file` configuration to the pinned runsc
fork. Keep upstream memfd behavior as the default.

1. The trusted runsc parent creates a generation-specific file beneath one
   configured, root-owned XFS directory. It validates the sandbox ID and
   generation, uses no caller-supplied path, resolves it with `openat2`
   `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS`, opens with `O_CLOEXEC` and mode
   `0600`, and verifies the regular-file owner and device.
2. Add `ApplicationMemoryFile *os.File` to `sandbox.Args`, donate it as
   `application-memory-fd`, receive it in the internal `boot` command, and pass
   ownership through `boot.Args`.
3. Change `createMemoryFile` to use the donated file and
   `MemoryFileOpts{DiskBackedFile: true}`. Keep the existing memfd path
   byte-for-byte equivalent when the option is disabled.
4. Leave cleanup to the root-owned backing-store lifecycle, rather than
   `DecommitOnDestroy`: destroying a sentry must not be allowed to punch out a
   file retained for a later hibernation restore.
5. Disable application THP initially. Regular-file THP behavior is not
   equivalent to shmem THP and must be enabled only after its own latency and
   amplification measurements.

The first spike may create the file directly in runsc. Production must bind it
to the existing sandbox generation intent and project-quota setup before
Docker creation so a stale runtime invocation cannot adopt another
generation's data. The file is passed only as an FD and is never visible in the
sandbox filesystem. The backing mount must also be tested for the executable
mapping behavior required by systrap; applying `noexec` is not assumed safe.

### Runtime patch 2: explicit pageout RPC

Add a separate `runsc park <sandbox-id>` command and controller RPC; do not
change the semantics of the OCI `pause` command.

The sentry-side operation must:

1. reject memfd-backed sandboxes and concurrent save/restore;
2. pause all sandbox tasks and wait for kernel quiescence;
3. issue `MADV_PAGEOUT` over every mapped chunk in the main `MemoryFile`;
4. use `mincore` plus host-cgroup measurements to report resident, dirty,
   written, and remaining bytes;
5. optionally follow with `fsync` and `MADV_DONTNEED` in the durable variant;
6. leave the kernel paused and return a structured partial result if Linux
   ignores some pageout advice.

`MADV_PAGEOUT` is deliberately a best-effort primitive: on file-backed dirty
pages Linux writes data to the backing file, but advice may be ignored for
some pages. The node state may say `parked_disk` only after it observes the
configured resident target or records the explicit partial result. The
operation must be idempotent, because recovery can repeat it after an
ambiguous timeout.

Private per-sandbox `MemoryFile`s used by tmpfs or overlay filestores must be
enumerated and included before production. Shared image/gofer cache must not be
blindly discarded; sharing those clean pages across sandboxes is useful.

Resume remains the existing runsc resume path. The node admits a wake into the
active-tool queue, changes the durable state to `waking`, invokes resume, and
allows exec/file activity only after the runtime reports running.

### Node lifecycle and persistence

Extend `SandboxRecord` rather than introducing another state database:

```text
lifecycle_state: running | parking | parked_disk | waking | hibernating |
                 hibernated | restoring
park_generation: monotonic integer
backing_id: opaque generation-scoped identifier
backing_format: disk-memory-v1
runtime_fingerprint: exact custom runsc build and boot configuration
parked_resident_bytes, parked_allocated_bytes, parked_at
```

Use the existing `SandboxLifecycleCoordinator`:

- park, wake, hibernate, restore, fork, and delete take the exclusive lease;
- exec, read, write, and SSH take the shared lease and require `running`;
- persist `parking` or `waking` before invoking runsc;
- fence every completion by sandbox generation, operation ID, and
  `park_generation`;
- reconcile an unfinished intent by inspecting both Docker/runsc state and the
  generation-specific backing manifest.

Crash behavior for disk park is intentionally the same as a running sandbox:
if the sentry dies, the sandbox fails even though page bytes remain. A backing
file without a complete hibernation manifest is never independently
restorable and is garbage after runtime absence has been proven.

### Main uncertainties

The spike exists to answer these questions, not merely to demonstrate that a
container boots:

- Does regular-file backing regress hot application memory through writeback,
  XFS extent allocation, or different THP behavior?
- Can `MADV_PAGEOUT` actually remove pages that are also mapped into systrap
  subprocesses, or does it only reduce the sentry mapping's RSS?
- Does XFS file fault-in use useful readahead and I/O concurrency, or reproduce
  the approximately queue-depth-one swap behavior?
- Can the quota and reservation boundary eliminate every ENOSPC/SIGBUS path
  while allowing gVisor allocator metadata and internal pages enough headroom?
- Is the retained sentry/gofer floor low enough for the target sandbox count?
- For local hibernation, can Docker/containerd restart the same OCI identity
  cleanly, and can gVisor's logical memory accounting be separated from host
  residency without changing guest-visible limits?

### Benchmark before node integration

The external-backing and pageout patches must first be tested as a standalone
runsc build on the existing benchmark VM. Compare stock memfd plus swap with
file-backed resident and file-backed parked states.

Workloads:

- empty sandboxes at 1, 100, and 1,000 density;
- dense non-zero anonymous mappings of 256 MiB, 1 GiB, and 4 GiB;
- sparse 1 GiB and 4 GiB mappings with 4 KiB, 1 MiB, and 16 MiB touched;
- process-rich shells with pipes, open files, sockets, timers, and background
  children;
- main-memory tmpfs and disk-backed overlay mutations;
- 0%, 1%, 10%, and 100% of the working set dirtied between parks.

Measure:

- startup and steady-state application overhead from regular-file backing;
- pageout wall time, bytes written, dirty bytes at entry, and second-park cost;
- `memory.current`, `memory.stat` file/dirty/writeback fields, sentry RSS,
  `mincore` residency, XFS `st_blocks`, PSI, and device I/O;
- first `true`, `ls`, Python exec, sparse state access, and full sequential
  scan after wake;
- 1-, 24-, and 32-way synchronized wakes, then the complete 1,000-sandbox
  drain;
- random page hashes, process/FD/socket continuity, and park-wake-park loops;
- deletion and GC while resident, parking, parked, and partially woken.

The cold test may drop cache only on the dedicated VM and must report that
separately. Normal samples must use per-sandbox pageout; a host-wide
`drop_caches` result is not a production result.

### External-backing spike, 2026-07-27

The first custom runsc cut is implemented against the pinned gVisor commit.
It adds an opt-in regular-file backing FD for the main application
`pgalloc.MemoryFile`; the default runtime still uses a memfd. The patch,
focused test, Docker runtime configuration, and standalone benchmark are in
[`runtime/gvisor`](../runtime/gvisor/).

The benchmark ran on a fresh 32-vCPU AMD EPYC UCloud VM with 87 GiB usable
RAM, a 32 GiB swap file for the memfd baseline, and a dedicated 480 GiB XFS
filesystem. The XFS filesystem used a direct-I/O loop device over the VM's
single virtual disk so the loop backing file did not introduce a second page
cache. Both custom runtime entries used the same runsc binary and disabled
application THP. The custom binary passed
`TestCreateMemoryFileWithDiskBacking` and a Docker create/exec/destroy smoke
test, including backing-file cleanup.

For each live workload, the host paused the sandbox, timed `fsync` of the
regular backing file when present, reclaimed its cgroup, resumed it, and
checksum-verified every populated page. Dense results were:

| Populated mapping | memfd + swap scan | XFS scan | Fault-in rate, swap | Fault-in rate, XFS | XFS speedup | XFS resident before / parked |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 MiB | 1.92 s | 500 ms | 133 MiB/s | 512 MiB/s | 3.85x | 277 / 4.4 MiB |
| 1 GiB | 7.66 s | 1.77 s | 134 MiB/s | 579 MiB/s | 4.33x | 1,050 / 7.0 MiB |
| 4 GiB | 33.34 s | 7.78 s | 123 MiB/s | 526 MiB/s | 4.29x | 4,137 / 20.5 MiB |

Lightweight wake-up remained dominated by Docker/runsc control overhead, not
application page-in. Across the three dense XFS cases, `docker exec true` was
115--161 ms and `ls /` was 94--123 ms after park. The corresponding memfd
values were 133--283 ms and 85--108 ms. These are single directional samples,
not tail-latency claims.

Writeback behaved incrementally after the first park:

- the clean second `fsync` took 2.1--3.3 ms for 256 MiB through 4 GiB;
- after dirtying 1% of populated pages, `fsync` took 4.9, 9.4, and 27.4 ms;
- device writes during those 1%-dirty flushes were 2.8, 10.5, and 41.2 MiB,
  close to the newly dirty 2.6, 10.2, and 41.0 MiB;
- the initial `fsync` was only 63--175 ms because normal background writeback
  had already cleaned much of the file while the workload remained resident.

This confirms the main reason to use a canonical backing file: park need not
serialize or copy the entire working set, and Linux file faults use materially
more I/O concurrency than the measured swap path. Explicit writeback is
required before reclaim; cgroup reclaim alone correctly leaves dirty
file-backed pages resident.

The sparse result exposes the design's structural disadvantage. With a
logical 1 GiB mapping but only 1 MiB populated at evenly spaced offsets, the
checksum scan took 9.8 ms from memfd swap and 42.7 ms from XFS. End-to-end
`docker exec` plus scan was 92.5 versus 114.6 ms. File offsets preserve the
mapping's holes, so this access becomes 256 scattered file faults; swap packs
the populated pages more closely. Solving that generally requires prefetch
with possible read amplification or an indirection/compaction layer much
closer to AgentEnv's pager.

The result therefore clears the dense fault-in, reclaim, incremental-write,
and light-tool gates, but fails the provisional no-sparse-regression gate by
about 33 ms of mapping-touch time. It justifies keeping the backing patch as a
promising spike. It does **not** yet justify node/control-plane integration or
a production pageout RPC: those cannot repair the sparse fault layout. The
next experiment should use representative tool traces to decide whether the
absolute sparse penalty is acceptable, and compare bounded extent-aware
prefetch against its read amplification before expanding the fork.

Raw results are in
[`benchmarks/gvisor-disk-memory-2026-07-27.json`](benchmarks/gvisor-disk-memory-2026-07-27.json).

Provisional gates for proceeding:

- no page-content checkpoint file and no full-memory scan on park;
- at least 90% of application-resident bytes reclaimed for dense workloads;
- 24-way p95 for a lightweight tool below 750 ms after disk park;
- sequential fault-in at least 4x the measured approximately 120 MiB/s swap
  path, without regressing sparse access;
- park time and write volume scale with newly dirty bytes after background
  writeback, not total allocated memory;
- less than 5% steady-state CPU regression and less than 10% representative
  tool-runtime regression while resident;
- no data mismatch or lifecycle leak across 1,000 park/wake cycles and forced
  process failures.

Failing the fault-in or steady-state gates kills this design before control
plane work. In particular, if ordinary XFS faults still serialize near the
swap result, a custom backing file is not an AgentEnv substitute.

### Optional runtime patch 3: local hibernation

The standalone spike is now implemented because disk park passed its dense
fault-in and incremental-write gates and the retained sentry/gofer footprint
remained the density limit. Production integration is still optional and
gated.

The target artifact has one owner and one lineage:

```text
complete manifest
kernel/object metadata
canonical main MemoryFile
canonical private MemoryFiles
retained Docker writable layer
```

Required gVisor changes:

1. Add a `pgalloc.SaveOpts` external-backing mode that serializes
   `memoryFileSaved` allocation/refcount/chunk/accounting metadata without
   scanning or writing page contents.
2. Add a distinct constructor for an existing `MemoryFile`; the current
   `NewMemoryFile` unconditionally truncates its file to zero and is unsafe for
   restore.
3. Add a `LoadOpts` external-backing mode that validates the expected file
   identity and size, recreates chunk mappings, restores allocator metadata,
   and skips the committed-page read loop.
4. Separate logical guest memory accounting from host-page residency where
   necessary. The present `knownCommitted` state was designed around memfd
   commitment and must not cause restored disk pages to be charged as resident
   or repeatedly scanned incorrectly.
5. Pass the canonical main and private backing FDs into the existing restore
   path instead of creating new empty files.

Hibernation ordering:

1. persist a generation-fenced `HIBERNATING` intent while the current sentry
   remains authoritative;
2. take the sandbox mutation lease, quiesce tasks, networking, filesystem
   state, and asynchronous page loads, then serialize kernel and allocator
   metadata into a private pending generation;
3. `syncfs` the writable container filesystem and fsync any private page
   images that are not canonical regular-file backings;
4. hard-stop and reap the sentry. From this point the pending generation, not
   a live process, is authoritative and recovery must be able to finish it;
5. rename the canonical backing inode into the pending generation, validate
   its identity and exact logical size, fsync the files and directories, and
   write a versioned manifest containing the generation, spec hash, file
   identities and sizes, CPU/page-size requirements, runsc digest, and boot
   fingerprint;
6. atomically publish `COMPLETE`, fsync its parent, and commit `PARKED`;
7. on wake, persist a generation-fenced `RESTORING` intent, reopen only files
   named by the complete manifest, and start one candidate sentry;
8. keep tool traffic fenced until the exact candidate passes the existing
   readiness/identity hook, then atomically commit `RUNNING` and reclaim
   obsolete metadata.

The manifest authenticates its small metadata but does not hash the full
application-memory file: a full content digest would reintroduce the scan this
design removes. Root-only directories, confined opens, exact inode/type/size
checks, generation fencing, and the gVisor metadata graph protect the local
canonical file.

Recovery distinguishes three crash regions. Before the sentry is reaped it
may abort the pending generation and resume the exact paused process. After
the sentry is reaped it must finish publishing or explicitly fail the pending
generation; it must never guess that the old runtime is alive. During restore
it either adopts the exact discovered candidate as `RUNNING` or kills it and
recovers the still-fenced artifact. No path may make both a sentry and a
parked generation authoritative.

This stage must pass stock checkpoint/restore conformance plus repeated
hibernate/restore of process trees, signals, pipes, sockets, epoll, timers,
tmpfs, deleted-open files, and a second hibernation immediately after a sparse
wake. Exact runsc build compatibility is mandatory; there is no best-effort
restore across versions.

### Local-hibernation spike, 2026-07-27

The pinned runsc fork now implements the narrow, linear version of this
design:

- `pgalloc` can save allocator metadata without scanning/copying application
  pages and can map an existing, size-validated regular backing file;
- `runsc checkpoint --hibernate` writes the ordinary kernel object graph,
  emits no main-memory page image, keeps guest tasks and timekeeper updates
  quiesced, hard-stops the sentry, and atomically renames the backing inode
  into the checkpoint directory;
- local restore donates that inode as the main `MemoryFile`, background-loads
  any private memory files, resumes the process tree, and consumes the
  single-owner artifact by renaming it back to active storage;
- a failed size check occurs before allocator state is adopted;
- the default memfd and stock checkpoint paths are unchanged.

Stopping the sentry cleanly was not correct. gVisor normally resumes internal
time updates after serialization and unpauses/kills the guest, which changed
the vDSO page and punched holes while tearing down guest mappings. A canonical
backing file therefore requires quiescing both runtime and guest through
process termination. This is a substantive semantic difference from stock
checkpoint, not just a faster `SaveTo`.

The dedicated 32-vCPU, 96 GB UCloud VM ran three checksum-verified cycles for
each case. Cycle 1 was warm, cycle 2 called `sync` and dropped the host page
cache, and cycle 3 was warm again:

| Mapping | First dirty park | Later park range | Warm restore | Cold restore | Warm first response | Cold first response |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 MiB dense | 203 ms | 55--57 ms | 164--165 ms | 166 ms | 20--25 ms | 24 ms |
| 1 GiB dense | 577 ms | 93--117 ms | 165 ms | 218 ms | 17--21 ms | 28 ms |
| 4 GiB dense | 2.05 s | 266--334 ms | 165 ms | 268 ms | 19--20 ms | 27 ms |
| 4 GiB logical / 1 MiB populated | 53 ms | 46--52 ms | 165--215 ms | 267 ms | 19--21 ms | 29 ms |

Restore time is effectively independent of populated size because it maps the
file and reconstructs metadata; it does not read the working set. The deferred
page-in cost remains real:

| Mapping | Warm checksum scan | Cold checksum scan |
| ---: | ---: | ---: |
| 256 MiB dense | 70--76 ms | 454 ms |
| 1 GiB dense | 207--209 ms | 1.77 s |
| 4 GiB dense | 707--884 ms | 8.92 s |
| 4 GiB logical / 1 MiB populated | 27--30 ms | 96 ms |

Every parked sentry PID was gone, every restore preserved the initial
in-memory checksum, `pages.img` was zero bytes, and `checkpoint.img` was about
224 KiB. The first dense park still scales at roughly 0.5 seconds per GiB
because the new regular file is dirty and process teardown must discard a
large resident mapping. Later read-mostly parks are much cheaper. Even the
2.05-second first 4 GiB park is roughly 27x below the 54--59 second stock
checkpoint measured on this host.

The allocator grows its backing in 1 GiB chunks. Observed logical file sizes
were 1 GiB for the 256 MiB case, 2 GiB for 1 GiB, and 5 GiB for 4 GiB. Holes
consume no blocks, but production admission cannot rely on that: the bounded
runtime allowance must include up to one chunk of allocator slack in addition
to the complete memory and writable-disk guarantees.

This result is close to AgentEnv for the linear park/wake use case: steady
park is 46--334 ms, restore is 165--268 ms, and the first lightweight guest
response is below 29 ms even cold. It deliberately does not implement
AgentEnv's forkable layered snapshots, dirty-page diffs, ublk/OverlayBD
storage, remote images, or crash-durable generations. Those mechanisms become
necessary if the product needs fan-out, cross-node restore, or replayable
artifacts; adding them to prove single-owner local hibernation would add risk
without improving its page-fault path.

The runsc artifact still requires a same-filesystem rename, consumes the
application-memory artifact on restore, and does not itself fsync the v1
manifest. The repository now separately implements generation fencing,
durable manifest publication, startup reconciliation, exact runtime
fingerprints, conservative disk reservation, and XFS project ownership. The
direct-runsc process/FD/socket matrix passed 1,000 cycles, but those control
plane primitives are not yet joined to the privileged runtime operation.
Private-`MemoryFile` coverage and a production-safe runtime owner remain open.

The patch, driver, and reproduction notes are in
[`runtime/gvisor`](../runtime/gvisor/). Raw results are in
[`benchmarks/gvisor-hibernate-2026-07-27.json`](benchmarks/gvisor-hibernate-2026-07-27.json).

### Durable publication benchmark, 2026-07-28

The initial hibernation numbers ended after renaming the canonical memory
inode. Manifest v1 correctly requires the artifact files to be durable before
publishing `COMPLETE`, so a matched 32-vCPU Zen5 UCloud VM measured
`mmap.flush()` plus `fsync()` after newly dirtying every 4 KiB page:

| Dirty backing | Durable seal range | Seal bandwidth range |
| ---: | ---: | ---: |
| 256 MiB | 107--111 ms | 2.31--2.39 GiB/s |
| 1 GiB | 423--446 ms | 2.30--2.42 GiB/s |
| 4 GiB | 1.58--2.03 s | 2.02--2.59 GiB/s |

This is a deliberately pessimistic all-pages-newly-dirty seal. It shows that
durability is not free, but also that the earlier roughly 73 MiB/s checkpoint
rate came from page serialization/copying rather than the storage device's
flush bandwidth. Steady park still needs a representative dirty-page
distribution; it must not quote the 46--334 ms non-durable park latency as the
complete production latency.

The reusable benchmark is
[`runtime/gvisor/benchmark_fsync.py`](../runtime/gvisor/benchmark_fsync.py);
raw results are in
[`benchmarks/gvisor-hibernate-fsync-2026-07-28.json`](benchmarks/gvisor-hibernate-fsync-2026-07-28.json).

### Crash and Docker lifecycle gates, 2026-07-28

The deterministic restart matrix reopens the journal and artifact store after
eleven persistence regions: live and dead hibernate boundaries, the
two-phase `COMPLETE`-while-sentry-live boundary, complete-before-commit,
restore-before-launch, candidate launch before identity publication, live/dead
candidate handling, and running commit. The Linux run completed 1,000 cycles
per region (11,000 restarts total). It also found and closed the
candidate-launch gap: startup now resolves and durably adopts an already
running candidate before restore can be retried. Raw results are in
[`benchmarks/gvisor-crash-recovery-1000-2026-07-28.json`](benchmarks/gvisor-crash-recovery-1000-2026-07-28.json).

The stock Docker/containerd gate failed decisively. The pinned runtime started
normally under Docker with a separate shim, gofer, sentry, writable layer, and
quota-owned application-memory inode. Hibernation completed in about 30 ms,
but reaping the sentry made containerd:

- mark the Docker task exited with code 137 and PID 0;
- stop the shim and gofer; and
- remove the transient OCI bundle.

`docker start` then booted fresh process state from the retained writable
layer; it did not restore memory. The result is recorded in
[`benchmarks/gvisor-docker-gofer-2026-07-28.json`](benchmarks/gvisor-docker-gofer-2026-07-28.json).
Consequently P3 does not pass when Docker owns each sandbox task, and P4 must
not begin on that ownership model.

Keeping the ordinary Docker task resident is not a cheap substitute on this
stack. Before adding a 96 MiB guest working set, the shim, gofer, and sentry
used about 9.8, 25.0, and 36.0 MiB RSS respectively: roughly 70.7 MiB of
per-sandbox runtime floor. One thousand such sandboxes would consume about
69 GiB before guest working sets and node overhead, substantially worse than
the roughly 16 MiB/cell density cited by the direct Warden experiment.

There are only three credible continuations:

1. retain the sentry/shim/gofer and rely on ordinary host reclaim, which gives
   up most of the hibernation density gain;
2. add a stable in-runtime supervisor and in-place sentry reconstruction,
   which is a substantially larger gVisor lifecycle fork; or
3. own the parkable runtime outside stock Docker (a Warden/containerd shim),
   which reintroduces the custom runtime the experiment was trying to avoid.

### Selected ownership model: one node Warden

The selected continuation is narrower than a containerd runtime shim and
matches AgentEnv's actual boundary: one privileged daemon owns every sandbox
backend on a node. Docker remains image infrastructure; it never owns a
parkable sandbox task.

This is a replacement architecture, not a second runtime tier. A sandbox node
runs exactly one task-lifecycle implementation. During qualification, whole
drained nodes may run either the legacy Docker-owned release or the direct
Warden release, but a node never admits a mixture and no sandbox-level runtime
selector exists. After the direct release passes the production-flow gates,
the legacy task runtime is deprecated and removed. Docker/containerd remain
only for image pull/build and content-addressed layer caching.

```text
control plane / tool traffic
           |
           v
  privileged node Warden
    | durable lifecycle journal + quota ledger
    | direct runsc create/checkpoint/restore/exec/delete
    |
    +-- sandbox A: runsc sentry + gofer + durable OCI bundle
    +-- sandbox B: parked artifact only (no sentry/gofer)
    +-- sandbox C: runsc sentry + gofer + durable OCI bundle

Docker image/build/layer cache
           |
           +-- mount immutable layer views for the Warden
               (never starts the sandbox task)
```

This follows four invariants from the AgentEnv backend/orchestrator split:

1. paused backend state is opaque to the outer scheduler;
2. capture and stop are distinct operations;
3. paused state and artifact ownership become durable before the live backend
   is stopped; and
4. resume constructs a new backend instance from the opaque paused state.

For gVisor, capture must therefore become two-phase:

1. quiesce tasks and time, serialize kernel/allocator metadata, rename the
   canonical backing inode into a pending generation, and return while the
   sentry remains paused;
2. the Warden fsyncs artifacts, publishes `COMPLETE`, and commits the paused
   metadata;
3. only then does the Warden SIGKILL the exact sentry PID/start-time identity,
   wait for death, and delete the direct-runsc runtime/gofer state;
4. resume uses the retained durable OCI bundle and complete generation to
   launch a new direct-runsc sentry/gofer, verifies readiness, then commits
   `running`.

A capture failure before durable publication can rename the backing inode
back to its active name and issue `runsc resume`. A failure after publication
is terminal for the old live backend: recovery must finish stopping it and
retain the paused generation. This ordering removes Docker/containerd from
the task lifecycle and closes the data-loss window in the original
kill-inside-checkpoint spike.

### Prioritized production plan

This order is based on dependency and loss-of-state risk, not implementation
convenience. P0 and P1 can be developed partly in parallel, but no sandbox
should be advertised as parkable until every P0--P3 exit gate passes.

Implementation status as of 2026-07-28: the pinned three-patch build,
content-addressed runtime, focused upstream tests, v1 manifest/fingerprint,
fenced journal, strict startup inventory, 11,000-restart kill matrix,
conservative reservation ledger, XFS project helper, and explicit
disabled-by-default legacy node wiring exist. Disk overcommit above 1x is
rejected across configuration, heartbeat construction, node admission, and
scheduler capacity.

The containerd ownership blocker has been removed from the selected path. A
new privileged `DirectRunscWarden` owns direct runsc create, checkpoint,
restore, exec, reconciliation, and delete. `DockerOverlay2RootfsStore` uses
Docker only for image resolution and content-addressed layer ownership, pins
the immutable image ID, and mounts its existing overlay2 layers without
flattening them; `OverlayRootfsManager` creates a per-sandbox writable overlay
and durable OCI bundle. A disabled `DirectNodeCoordinator`, tied to exact runsc and
boot-configuration digests, serializes each sandbox lifecycle and bounds
node-wide restores. Caller cancellation cannot abandon an in-flight restore.

Measured on the 32-vCPU UCloud node:

| Gate | Result |
| :--- | :--- |
| Two-phase Warden, 100 stateful cycles | park p50/p95 61.7/66.1 ms; resume plus readiness 142.4/150.8 ms |
| Docker image-only path, BusyBox | cold export 321 ms; cached lookup 32 ms; overlay prepare/release 40/7.9 ms; no exporter leak |
| Zero-flatten Docker overlay2 path, 1.7-GiB pandas | inspect/lease/image mount/sandbox mount 16.7/20.4/3.5/3.3 ms; copy-up verified; no archive or extracted copy |
| 24-way stateful wake burst, 4 slots | median makespan 1.01 s; request p50/p95 600/998 ms |
| 24-way stateful wake burst, 8 slots | median makespan 570 ms; request p50/p95 396/557 ms |
| 24-way stateful wake burst, 24 slots | median makespan 390 ms; request p50/p95 350/387 ms |
| 256 live stateful sandboxes | 8.35 GiB total sentry RSS; 33.4 MiB per-sandbox median |
| Same 256 parked | zero retained sentries; 4.16 GiB allocated artifacts; 256 GiB logical memory files |

The stateful workload is not an empty shell: every sandbox carries a 16 MiB
non-zero mapping, child process, threads, pipes, Unix and TCP sockets,
epoll/eventfd, timerfd, futex/condition state, a deleted-open file, and signal
state. Every tool-triggered restore verifies all of it. The density result also
reinforces the disk invariant: sparse allocated blocks were small, but
admission still owes the full 1 GiB logical memory limit per sandbox.

Raw results are in
[`benchmarks/gvisor-warden-two-phase-2026-07-28.json`](benchmarks/gvisor-warden-two-phase-2026-07-28.json),
[`benchmarks/gvisor-image-rootfs-2026-07-28.json`](benchmarks/gvisor-image-rootfs-2026-07-28.json),
[`benchmarks/gvisor-direct-node-stateful-burst-2026-07-28.json`](benchmarks/gvisor-direct-node-stateful-burst-2026-07-28.json),
and
[`benchmarks/gvisor-direct-density-256-2026-07-28.json`](benchmarks/gvisor-direct-density-256-2026-07-28.json).

#### P0: Freeze the runtime and artifact contract

First turn the patch into a reproducible gVisor fork and define the format that
all later recovery logic consumes.

- pin the upstream commit, toolchain, build flags, and patch series; publish a
  content-addressed runsc binary and retain symbols;
- define manifest v1 and the `RUNNING -> HIBERNATING -> PARKED -> RESTORING`
  state machine, including legal owner/generation transitions and every crash
  point;
- fingerprint runsc, platform, CPU features, page size, boot configuration,
  rootfs identity, and sandbox spec; fail closed on any mismatch;
- bind the exact runtime to a node-wide deployment identity; qualification may
  compare whole drained legacy and direct nodes, but one process and one node
  must never serve both task-lifecycle implementations;
- add allocator tests for malformed metadata, wrong file type/size, truncated
  files, repeated restore, and failures before live allocator mutation.

**Exit:** a clean checkout reproduces the exact binary and artifact schema;
incompatible artifacts are rejected before a sentry is started; upstream
tests plus focused allocator tests pass. This is approximately one
engineer-week and is the prerequisite for meaningful durability work.

#### P1: Make one local generation crash-safe

This is the highest-risk production feature. Implement the fenced lifecycle
and reconciliation before scheduler or parking policy work.

- create root-owned generation directories through dirfd-relative
  `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS)` calls;
- capture into a pending generation while the exact sentry remains alive and
  paused; fsync payloads and metadata, atomically publish `COMPLETE`, and only
  then reap the sentry and delete its direct-runsc runtime state;
- make every operation idempotent by sandbox incarnation, operation ID, and
  generation; serialize create/exec/upload/delete/park/wake with the existing
  sandbox mutation lease;
- add startup reconciliation for every crash region, including a dead sentry
  with a pending generation and a live restored candidate before the
  `RUNNING` commit;
- retain exactly the metadata needed to adopt or kill an ambiguous restore;
  never silently fall back to stock runsc or discard the only canonical
  memory inode.

**Exit:** deterministic kill tests at every persistence boundary complete
1,000 cycles without double-running a sandbox, losing the canonical backing,
or leaking a generation. Until this passes, the feature is data-loss-prone
regardless of its benchmark latency.

#### P2: Enforce hard physical disk admission

Disk cannot be overcommitted. Integrate the memory backing into the same
fail-closed capacity model as writable sandbox storage.

- reserve, per admitted sandbox, requested writable disk + the chunk-rounded
  main memory backing + a second full memory allowance for private page files
  + bounded metadata overhead; reduce the private allowance only after P3
  proves a smaller universal bound;
- charge the backing and writable layer to a sandbox-owned XFS project quota,
  while keeping shared immutable image layers outside the per-sandbox charge;
- reconcile scheduler reservations, quota limits, physical allocation, and
  generation cleanup after crashes; fail admission when any measurement is
  unavailable;
- maintain node safety headroom and expose logical reservation separately from
  currently allocated sparse-file blocks; sparse holes never create capacity;
- prove ENOSPC/quota failures before, during, and after hibernation cannot
  corrupt the previous authoritative state.

**Exit:** aggregate worst-case reservations never exceed usable node storage,
and fault injection at the quota boundary fails closed. This gate, rather
than average sparse usage, determines the homogeneous sandbox count.

#### P3: Complete runtime correctness and conformance

Qualify the narrow local format against real sandbox state before connecting
it to tool traffic.

- inventory every gVisor `MemoryFile`; either externalize private files or
  retain their ordinary page image with an explicit hard size bound;
- exercise process trees, signals, pipes, Unix and network sockets, epoll,
  timers, futexes, tmpfs, deleted-open files, mmap variants, and repeated
  hibernation immediately after a sparse wake;
- verify direct-runsc gofer behavior when the sentry is reaped, the durable
  overlay/bundle is retained, the Warden restarts, and delete races park or
  restore; Docker/containerd task ownership is explicitly out of scope;
- benchmark resident regular-file overhead, write-heavy workloads, 1,000-cycle
  dirty-page behavior, and synchronized cold wakes using representative RL
  traces rather than empty sandboxes;
- establish p95/p99 gates for park, first tool response, full working-set
  fault-in, disk queueing, and ordinary tool runtime.

**Exit:** no unsupported state is silently accepted; 1,000-cycle and
representative-load suites pass; steady-state CPU regression is below 5% and
representative tool-runtime regression below 10%. A failed state class can be
made explicitly non-parkable rather than delaying the entire feature.

#### P4: Integrate bounded park/wake orchestration

Only after the artifact is safe should the node agent and gateway route tool
calls through it.

- add durable node records for the four lifecycle states and expose parked
  sandboxes in complete heartbeat inventory without counting them as resident
  processes;
- queue the triggering tool request behind one idempotent wake operation,
  restore once, run readiness, then release traffic; propagate cancellation
  without abandoning an ambiguous restore;
- bound concurrent restores and cold page-in pressure using disk latency/PSI
  feedback and reserve active memory according to the existing overcommit
  policy;
- make delete, TTL expiry, drain, node restart, and deployment replacement
  understand parked generations;
- keep the capability fail-closed and tied to the exact live conformance
  fingerprint.

**Exit:** duplicate tool calls cause one wake, synchronized inference batches
cannot saturate the node into unbounded tail latency, and drain/delete/restart
settle every sandbox into one authoritative terminal or running state.

#### P5: Add conservative policy, observability, and canary rollout

Start with longer-idle full hibernation; do not park on every model turn until
real traces justify it.

- initially require an idle interval and no active exec/file/build mutation;
  prioritize high-resident-memory sandboxes with long predicted idle periods;
- add hysteresis and a minimum resident interval after wake to prevent
  park/wake oscillation;
- record park/wake latency, generation size, allocated blocks, bytes dirtied,
  major faults, disk latency/queue depth, restore concurrency, reconciliation
  outcomes, and fallback/failure reasons;
- canary on dedicated direct-runtime nodes, then ramp by node and tenant;
  automated rollback closes admission and drains those whole nodes without
  moving their live or parked generations to the legacy runtime;
- compare achieved sandboxes/node, tool p95/p99, disk headroom, and failure
  rate against the current memory-overcommit design.

**Exit:** a multi-day canary demonstrates a material density gain without
violating the latency SLO or disk invariant. Only then consider parking during
ordinary model-generation gaps.

#### P6: Preserve image layers; defer the remaining AgentEnv expansion

The production flattening regression demonstrated one requirement early:
ordinary OCI image layers must remain shared rather than being exported and
extracted into a merged directory. The immediate implementation mounts
Docker's already-local overlay2 chain as the immutable lower. This gives the
Warden the same essential layer-preservation property without adding a major
gVisor patch or a second image converter.

Do not yet put registry-range-backed OverlayBD image lowers, layered/dirty-page
memory formats, remote snapshots, or forkable memory on the critical path. Add
them only for a demonstrated product requirement:

- OverlayBD/registryfs image lowers when registry pull, rather than local
  flattening, is the measured cold-start bottleneck;
- reflink lineage for fan-out or replayable local snapshots;
- layered/dirty-page generations when repeated artifact retention, rather than
  single-owner park/wake, dominates write cost;
- remote backing and a userspace block path for cross-node restore or durable
  snapshot distribution.

Each child or retained generation still needs a hard worst-case COW/disk
reservation. These features may reduce copying and transfer latency, but they
do not increase safe admission capacity by themselves.

Rollback closes admission on direct-runtime nodes and drains existing
file-backed generations on the exact custom binary. A sandbox is never
switched to the legacy runtime in place. Once production qualification passes,
remove the legacy Docker task lifecycle, its checkpoint wrapper/helper, and
its runtime-specific configuration rather than maintaining two implementations.

### Legacy removal gate

The direct runtime is not ready merely because parking works. It must pass the
production-flow matrix in
[`direct-runtime-production-qualification.md`](direct-runtime-production-qualification.md)
on dedicated nodes. Removal requires:

1. parity or an explicit product decision for create, exec/streaming/TTY,
   files, networking, SSH, snapshots, TTL, drain, delete, recovery,
   accounting, and observability;
2. a clean-node production canary and a node-restart/drain exercise with no
   leaked process, mount, quota, bundle, or generation;
3. control-plane routing that rejects mixed runtime deployment identities;
4. rollback tested as whole-node admission close and drain;
5. legacy state drained to zero before its code, units, helpers, flags,
   capabilities, and conformance probes are deleted.

Fork is explicitly deferred from the initial replacement release. It is not a
reason to retain or co-run the legacy runtime. A later fork implementation
should use native Warden artifact ownership and reflink/snapshot lineage.

Order-of-magnitude sizing, not a delivery commitment:

- the backing-FD and metadata-only hibernation performance spike is complete;
- P0 reproducible fork/contract: roughly one engineer-week;
- P1 crash-safe generation and recovery: roughly one to two engineer-weeks;
- P2 hard disk accounting/quota: roughly one engineer-week;
- P3 conformance and representative qualification: roughly one to two
  engineer-weeks, overlapping late P1/P2 work;
- P4/P5 orchestration, metrics, and canary: roughly two engineer-weeks;
- maintaining, rebasing, and requalifying a custom gVisor fork remains the
  ongoing cost and the largest schedule risk.

## Rollout gates

Do not enable per-turn parking from claimed first-instruction latency alone.
Proceed only when representative workload traces establish:

1. acceptable resident-mode overhead from regular-file application memory;
2. park cost proportional to newly dirty pages and a low-cost second park;
3. acceptable first-tool, sparse-touch, and full-touch p95/p99 under
   synchronized wakes;
4. physical disk reservation for memory plus writable-disk guarantees;
5. bounded active-tool scheduling under a complete inference batch;
6. fail-closed recovery after node-agent, dockerd, wrapper, and runsc crashes.

Until those gates pass, background restore remains a live-fork latency
optimization and longer-idle parking remains selective rather than automatic
on every model turn.
