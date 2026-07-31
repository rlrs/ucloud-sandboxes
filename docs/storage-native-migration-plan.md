# Storage-native sandbox mobility plan

Status: **Production rollout complete**

Last reviewed: 2026-07-31

This document is the source of truth for the storage-native migration work.
Before changing the storage format, device lifecycle, park/resume protocol,
publication protocol, placement model, or benchmark target, reread this
document and record the decision in the decision log. Do not optimize a
different problem merely because it is easier to measure.

Post-rollout priorities are tracked only in
[Production roadmap](roadmap.md). This document is the completed design and
qualification record, not a second active backlog.

## Goal

Replace whole-archive sandbox migration with an AgentEnv-like storage model
that makes a parked sandbox a durable, content-addressed snapshot rather than
a node-local tar file.

The change must improve both:

1. migration latency, especially the synchronous path before route handoff;
2. source and destination node load, including CPU time, disk read/write
   amplification, and required network bytes.

Faster migration that merely moves the same work into foreground page faults
or unconstrained storage workers does not pass.

## Non-goals

- Do not replace gVisor with Firecracker or require nested virtualization.
- Do not reintroduce Docker/containerd ownership of sandbox task lifecycles.
- Do not implement fork before linear park/resume and migration are qualified.
- Do not weaken hard disk admission. A client's requested writable limit must
  remain usable at the hard limit.
- Do not add a large gVisor patch until the regular-file-on-COW-device
  compatibility gate proves that one is necessary.
- Do not retain two production task runtimes. The storage-native backend is a
  versioned evolution of the direct Warden and must replace its archive
  migration path after qualification.

## Required invariants

The existing direct-runtime ownership invariants remain mandatory:

1. One privileged Warden owns every sandbox backend on a node.
2. A sandbox has exactly one authoritative live or parked generation.
3. The old source remains authoritative until a durable snapshot is published
   and the gateway commits the route/placement transition.
4. A destination never publishes or executes from an unverified generation.
5. Every lifecycle boundary is replayable after process or node failure.
6. Disk admission never exceeds guaranteed physical or durable remote
   capacity.
7. Cache bytes are disposable acceleration and never count as authoritative
   capacity.
8. A source node may disappear after snapshot publication without making the
   sandbox unrestorable.

Storage-native invariants:

1. Sandbox writable filesystem state and the external gVisor application
   memory file live on one fixed-size filesystem backed by a COW block device.
2. Parking quiesces gVisor, flushes and freezes that filesystem, seals its
   current writable layer, and publishes a content-addressed manifest before
   releasing the backend.
3. Resume constructs a fresh writable COW layer over immutable parents,
   mounts the filesystem, and gives runsc ordinary local paths. gVisor must
   not know whether missing blocks are local or remote.
4. Remote reads are authenticated by immutable layer identity and bounded by
   the sandbox device geometry.
5. Foreground block faults, background publication, cache fill, and peer
   serving all have explicit concurrency and bandwidth limits.

## Intended architecture

```text
                         gateway / autoscaler
                                  |
                     durable snapshot manifest
                                  |
          +-----------------------+-----------------------+
          |                                               |
   source node Warden                              destination Warden
          |                                               |
   storage daemon (one/node)                      storage daemon (one/node)
          |                                               |
   seal writable COW layer                       create fresh writable COW
          |                                               |
          +------ durable object/shared store ------------+
          +------ optional bounded P2P/cache --------------+

   runsc sees:
     <mounted device>/upper
     <mounted device>/active-memory/application-memory.bin
     <mounted device>/hibernate-N/...
```

The node storage daemon owns ublk devices, layer references, mounts, local
cache entries, I/O workers, and crash reconciliation. The Python Warden owns
sandbox lifecycle policy and talks to the daemon over a root-only Unix socket.
Docker remains image pull/export infrastructure only.

The first implementation should reuse an existing maintained ublk/overlaybd
implementation if its filesystem semantics, packaging, and process boundary
pass the compatibility gate. Reimplementing an asynchronous block stack in
Python is out of scope.

## Stage 0: compatibility and lower-bound spike

Build a destructive, isolated Linux harness. It must:

1. create a fixed-size layered block device and filesystem;
2. mount it and place both an overlayfs upper/work tree and a sparse regular
   application-memory file on it;
3. exercise mmap read/write, fsync, fallocate, hole punching, rename, xattrs,
   ACLs, hard links, symlinks, overlayfs copy-up, and hard ENOSPC;
4. quiesce writes, sync and freeze the filesystem, seal the writable layer,
   destroy the original mount/device, create a new COW device from the sealed
   layer, remount it, and verify all state;
5. run the actual pinned runsc stateful conformance workload through
   create/park/stop/seal/remount/restore/exec/delete;
6. trace restore block access to determine whether the first ordinary tool
   call reads only a bounded working set or effectively scans the entire
   memory backing;
7. measure CPU seconds, bytes read/written, wall time, and device I/O for every
   phase.

Go:

- all filesystem and gVisor conformance checks pass;
- requested device size fails writes with ENOSPC at the correct boundary;
- no full backing-file scan occurs before the first lightweight tool call;
- COW steady-state I/O is within 15% of native XFS for the representative
  workload;
- sealing does not copy the complete logical device.

Stop or redesign:

- mmap/fallocate/hole-punch semantics are incompatible;
- overlayfs upper is unsupported or unsafe;
- restore eagerly reads most of the memory backing;
- the only workable design requires one privileged process per sandbox;
- layer sealing is proportional to the complete logical device rather than
  newly dirtied extents.

## Stage 1: node storage daemon

Define a versioned root-only Unix-socket protocol with idempotent operations:

- `CreateVolume`
- `MountVolume`
- `FreezeAndSeal`
- `ReleaseRuntime`
- `AcquireSnapshot`
- `MountSnapshotCow`
- `Unmount`
- `DeleteVolume`
- `PublishLayer`
- `CacheInventory`
- `Reconcile`

Every operation is fenced by sandbox id, sandbox generation, operation id,
volume id, and expected revision. Responses include stable layer/device
identities and phase timings. The daemon persists a journal before mutating
device ownership and reconciles mounts, devices, layers, and references on
restart.

The daemon enforces:

- bounded device-acquire concurrency;
- bounded background publication and prefetch bandwidth;
- bounded P2P serving bandwidth;
- local cache high/low watermarks;
- exact logical device size;
- separate authoritative, writable, and disposable-cache byte accounting.

## Stage 2: storage-native park and resume

Add a node-wide `storage-native-v1` capability. Nodes either use the existing
direct archive storage layout or the storage-native layout; a node never mixes
layouts for newly admitted sandboxes.

Create:

1. reserve the complete requested writable capacity;
2. acquire and mount a fresh fixed-size COW volume;
3. place overlay upper/work, active memory, and hibernation artifacts on it;
4. create runsc through the existing direct Warden.

Park:

1. capture gVisor metadata while the sentry remains quiesced;
2. sync and freeze the mounted sandbox filesystem;
3. seal the current writable layer;
4. persist and publish the snapshot manifest;
5. commit `PARKED`;
6. reap the exact sentry and release mount/device resources.

Resume:

1. resolve and verify the snapshot manifest;
2. acquire its immutable layers and a fresh writable upper;
3. mount the filesystem;
4. restore runsc paused, journal the candidate, then resume;
5. commit `RUNNING`.

All old Warden crash-boundary tests must be repeated with injected daemon,
mount, freeze, seal, publish, acquire, and remote-read failures.

## Stage 3: durable publication and lazy reads

Snapshot manifests and immutable layers use content-derived identities.
Durable object/shared storage is authoritative. Optional P2P is an
acceleration path only.

Publication must upload only sealed layer bytes that are absent remotely.
It may run while a sandbox is parked, but route migration cannot discard the
last source authority until the manifest and required immutable layers are
durable.

Destination acquisition downloads only the manifest and indexes synchronously.
Block contents are fetched on demand through authenticated range reads and
stored in a bounded node-local cache. A small, measured hot set may be
prefetched; never prefetch the complete snapshot by default.

## Stage 4: manifest migration and placement

The gateway stores a snapshot manifest identity and storage schema with the
sandbox route. For a published parked snapshot:

1. choose a destination with writable hard capacity and compatible runtime;
2. ask it to acquire the manifest and create a fenced parked registration;
3. atomically commit the route;
4. retire the old node-local registration.

No full snapshot transfer is required before route commit. The source node is
not a required data server after publication.

Autoscaling accounts for:

- parked authoritative bytes in durable storage, not on a node;
- guaranteed local writable capacity required for a running/resuming sandbox;
- active CPU and memory;
- disposable cache pressure and eviction separately;
- bounded storage-daemon queues as an admission signal.

The v2 tar migration path remains readable for rolling upgrade and explicit
export/import. It is removed from normal placement after v1 storage-native
qualification.

## Benchmarks and acceptance gates

Use empty, realistic Verifiers, 256-MiB compressible, 256-MiB incompressible,
1-GiB resident, and filesystem-dirty workloads. Report p50/p95 and raw samples.
Compare identical source/destination nodes against release 0.3.64.

Required gates:

- at least 50% lower source CPU seconds per migrated allocated GiB;
- at least 50% lower destination write amplification;
- no whole-snapshot read or write in the synchronous migration path;
- park below 500 ms p95 for the representative workload;
- warm first tool call below 300 ms p95;
- remote first ordinary tool call below 2 seconds p95;
- route handoff below 100 ms after durable snapshot publication;
- background publication changes active tool-call p95 by less than 10%;
- source-node loss after publication does not affect resume;
- native-workload I/O regression remains below 15%;
- requested hard disk capacity remains fully enforceable;
- no leaked mount, ublk device, layer reference, cache pin, quota, or route
  after the crash and drain matrices.

Lower-bound stop rule:

If foreground resume is within 1.3 times the measured remote-block and local
filesystem lower bound, further custom storage work stops unless production
SLOs still require it. Do not add a larger gVisor patch merely to win a
microbenchmark.

## Rollout and rollback

Storage schema and node capability are explicit. The gateway schedules a
sandbox only onto a compatible schema. Rollout uses drained whole nodes.

Rollback before a snapshot is storage-native is ordinary node rollback.
After publication, rollback requires an explicit storage-native-to-v2 export
tool that materializes a portable archive without starting the sandbox.
Do not deploy storage-native nodes until that export path and its corruption
tests pass.

## Decision log

### 2026-07-30: prefer storage-native mobility over archive optimization

The measured 0.3.64 migration protocol spent 13.882 seconds in archive
transfer and staging, versus 8.043 ms in route commit. Optimizing tar would
retain whole-state transfer, source-node dependence, duplicate disk passes,
and node-pinned parked ownership. The selected direction is a layered,
content-addressed, lazy block path modeled after AgentEnv while retaining
gVisor.

### 2026-07-30: load reduction is a release gate

The design is accepted only if it reduces migration CPU, disk amplification,
and synchronous bytes in addition to wall-clock latency. P2P serving is
optional and rate-limited; authoritative storage must not depend on an
overloaded or disappearing sandbox node.

### 2026-07-30: the regular-file-on-COW-device storage semantics gate passed

On isolated DFM job `12362112`, the pinned AgentEnv ublk daemon at commit
`f41abb21324f6b0520abf34b7720aa260ddd10eb` passed the destructive 1-GiB
volume reconstruction test. Freeze plus seal took 29.3 ms, release took
26.2 ms, reconstruct plus mount took 9.4 ms, and the immutable layer was
35,725,312 bytes (3.33% of the logical device). mmap, fsync, fallocate, hole
punch, rename, xattrs, ACLs, hard and symbolic links, overlayfs copy-up, and
hard ENOSPC survived device destruction and reconstruction. Raw results are in
`docs/benchmarks/gvisor-storage-native-volume-2026-07-30.json`.

The UCloud Ubuntu image's matching `linux-modules-extra` package is required
because `CONFIG_BLK_DEV_UBLK=m` but the minimal base image does not ship
`ublk_drv`. This is a node-image packaging requirement, not a custom kernel.

The same destroyed/reconstructed volume passed the production runsc stateful
workload. Hibernate took 30.4 ms. After dropping the host page cache, restore
took 215.4 ms and read 565,248 bytes from ublk; the first static no-op tool took
16.7 ms and read another 102,400 bytes. Together that is 3.89% of the
17,145,856-byte allocated application-memory backing, so restore did not scan
the memory file. The subsequent full conformance call read 16,699,392 bytes as
expected while validating processes, threads, pipes, Unix and TCP sockets,
epoll/eventfd, timerfd, futex/condition state, deleted-open files, signals, and
the complete 16-MiB mapping. Raw results are in
`docs/benchmarks/gvisor-storage-native-conformance-2026-07-30.json`.

The gate does not yet establish steady-state I/O overhead; Stage 0 remains in
progress.

### 2026-07-30: use AgentEnv's hybrid runtime upper; Stage 0 passed

The pure append-only `logStructured` upper failed the steady-state I/O gate on
DFM job `12362112`: median sequential write bandwidth was 72.8% of native
loop-XFS and median random mixed IOPS was 73.1%. Metadata time was within the
gate at 101.4% of native. This mode is not the production candidate. Raw
results are in
`docs/benchmarks/gvisor-storage-native-io-log-2026-07-30.json`.

AgentEnv's default `hybridLogStructured` upper passed on the same job and
three-round alternating benchmark. Median sequential write bandwidth was
125.7% of native loop-XFS, random mixed IOPS was 111.2%, and metadata time was
97.1%. After the workload, restack sealed a 1,141,612,544-byte dirty layer in
11.6 ms without materializing the 4-GiB logical device. Raw results are in
`docs/benchmarks/gvisor-storage-native-io-hybrid-2026-07-30.json`.

The destructive filesystem and pinned production-runsc qualification also
passed in hybrid mode. The 1-GiB test produced a 52,969,472-byte sealed layer,
reconstructed all filesystem state, enforced hard ENOSPC, restored gVisor
after dropping host caches in 164.8 ms, and completed the first no-op in
14.8 ms. Restore plus no-op read 622,592 bytes from the device versus
17,145,856 allocated bytes in the application-memory backing. Raw results are
in
`docs/benchmarks/gvisor-storage-native-conformance-hybrid-2026-07-30.json`.

Hybrid layers cannot carry a precomputed append digest because existing upper
blocks may be overwritten in place. Foreground park therefore freezes and
restacks the compact dirty layer only. Durable publication must subsequently
stream that sealed layer once through hashing and upload, under explicit
background limits; it must not create another full staging copy or scan the
logical device. A manifest becomes durable only after the resulting digest,
size, and object identity are committed. This is consistent with the goal:
foreground migration avoids whole-state transfer, and background source load
is proportional to newly dirtied layer bytes.

All Stage 0 go conditions now pass. Proceed with the versioned node-daemon
lifecycle using `hybridLogStructured` as the runtime upper.

### 2026-07-30: wrap the pinned block backend with a UCloud node-storage service

The AgentEnv ublk daemon remains the pinned node-wide block backend. A small
UCloud node-storage service owns the production lifecycle boundary in front
of it: fenced/idempotent operations, the durable operation and volume journal,
mounts, authoritative/writable/cache accounting, concurrency limits, and
startup reconciliation. The direct Warden talks only to this versioned
service API.

This avoids both a per-sandbox privileged process and a large gVisor change.
The backend daemon and UCloud service are separate node-wide processes so the
service can restart and reconcile against a still-running block backend.
Devices are inventoried from the node's ublk sysfs namespace and mounts from
mountinfo; because storage-native nodes reserve ublk ownership for this
service, unjournaled devices are safe to classify as orphans. A missing
journaled live device is never silently recreated beneath a running sandbox:
it is surfaced as a terminal reconciliation result for the Warden to
quarantine.

The complete real service-boundary test passed on DFM job `12362112`. It
created and mounted a 1-GiB hybrid ublk/XFS volume through the root-only
versioned socket in 119.2 ms, wrote and fsynced a 256-MiB allocation, stopped and
recreated the UCloud service while leaving the AgentEnv backend and mount
alive, reconciled the mounted volume without error, sealed it in 37.0 ms, and
unmounted/deleted the runtime device in 37.8 ms. It then constructed a fresh
COW backend from the released snapshot in 24.3 ms, verified the payload,
sealed a second generation with both immutable lowers, and released it
cleanly. The first sealed layer allocated 67,584,000 bytes. Raw results are in
`docs/benchmarks/gvisor-storage-native-node-service-2026-07-30.json`.

### 2026-07-30: runsc cleanup precedes overlay detach and storage seal

The first production-Warden test through real hybrid ublk/XFS proved that
`checkpoint --hibernate` leaves the merged overlay detachable, but `runsc
delete` must subsequently remove its `.gvisor.filestore` from that merged
rootfs. Sealing and detaching the overlay before `runsc delete` therefore
caused deterministic cleanup failure.

The storage-native park transaction now publishes the complete local gVisor
checkpoint, reaps the exact sentry, runs `runsc delete` while the overlay is
still mounted, then detaches the overlay and freezes/seals/releases the
underlying volume before committing `PARKED`. This does not weaken authority:
after checkpoint publication the live backend is never resumed. A crash after
the sentry is reaped leaves checkpoint authority plus the still-mounted or
sealed volume, and reconciliation finishes cleanup and storage release before
the sandbox is externally usable as parked. The sealed layer now contains the
final filestore deletion and all overlay metadata written by runtime cleanup.

The corrected production-Warden path then passed on DFM job `12362112`.
Creating the gVisor runtime took 139.5 ms, park including checkpoint, exact
backend cleanup, overlay detach, seal, device deletion, and the `PARKED`
commit took 174.8 ms, and reconstructing a new device, remounting the overlay,
restoring paused, resuming, and passing the readiness call took 189.8 ms. The
first separate no-op took 15.8 ms and the full stateful conformance call
passed. The 1-GiB volume's sealed delta was 85,286,912 bytes. Final cleanup
left no ublk daemon, device, or mount. Raw results are in
`docs/benchmarks/gvisor-storage-native-warden-2026-07-30.json`.

### 2026-07-30: dense publication streams out of the block backend

Registry-backed OverlayBD lowers require a dense, sequential layer object.
The hybrid runtime layer is intentionally sparse/in-place and therefore
cannot be uploaded verbatim. AgentEnv's repository publisher currently
dense-exports to a temporary file and then uploads that file; carrying that
implementation over unchanged would add a complete local write of every
newly dirty byte to the source-node migration load.

On DFM job `12362112`, dense-exporting the real 85,319,680-byte sealed Warden
layer produced an 85,250,048-byte immutable object. Three cold-cache runs took
0.41, 0.44, and 0.45 seconds with 0.37, 0.39, and 0.37 CPU-seconds. Each run
read about 85 MiB and the temporary-file implementation wrote 85,250,048
bytes. The conversion is proportional to the dirty layer, not the 1-GiB
logical volume, but the temporary write is avoidable. Raw results are in
`docs/benchmarks/gvisor-storage-native-dense-export-2026-07-30.json`.

The selected publication boundary is a narrow addition to the pinned AgentEnv
ublk daemon: `ExportDenseLayer` writes the dense byte stream to a caller-owned,
root-only Unix socket and returns the content digest and size. The UCloud
storage service streams those bytes directly into a resumable registry upload,
verifies the independently computed digest and byte count, and journals the
published immutable descriptor. This retains one dirty-layer read, removes
the dense staging write, bounds memory to one upload chunk, and requires no
gVisor change. Publication remains background/bounded; the manifest is not
durable until the registry confirms the content-addressed blob.

The subsequent remote-read format check found that AgentEnv's current
registry reader unconditionally applies its single-file tar adaptor, while
its dense snapshot publisher writes a raw dense LSMT file. Those two paths
are not wire-compatible. Wrapping a one-pass stream in tar would require
knowing the final dense length before sending its header, forcing either a
second source read or complete buffering. The pinned storage patch therefore
adds a narrow reader fallback: if tar validation fails, OverlayBD tries the
same immutable blob as raw dense LSMT, whose own header/index/trailer
validation remains authoritative. Existing tar-wrapped container image layers
are unchanged. Snapshot manifests label these blobs with UCloud's raw dense
OverlayBD media type. This is an OverlayBD/backend compatibility change, not a
gVisor runtime change.

The complete UCloud node-service transaction then passed against a local
Registry-v2 protocol server on DFM job `12362112`: the 67,584,000-byte sealed
hybrid layer was durably published in 347.8 ms. The node-local layer and
complete source volume registration were then deleted. A separate destination
journal verified and acquired the durable manifest, and its fresh-cache COW
device acquired the remote raw layer in 39.2 ms. Mounting and reading both
sentinels fetched 2,764,983 bytes over 14 range requests. A second local delta
sealed successfully over the published parent. The source transaction used
no dense staging file. Raw results are in
`docs/benchmarks/gvisor-storage-native-publication-2026-07-30.json`.

An unpublished `RELEASED` volume continues to reserve its full requested
writable size in node admission. Only the durable `PUBLISHED` transition
releases that reservation; otherwise multiple locally authoritative parked
volumes could overallocate physical disk despite having no live device.

### 2026-07-30: migrate portable metadata plus a storage manifest, not an archive

Normal storage-native migration keeps the existing fenced gateway protocol
but replaces the v2 archive identity and source download URL with a
`storage-native-v1` descriptor. The descriptor contains the durable storage
publication and the existing portable direct-runtime migration metadata
(sandbox spec, runtime identity, hibernation generation, artifact roles and
logical sizes, and network disconnect policy). Its canonical SHA-256 is the
migration fence stored by both nodes and the gateway.

The destination verifies and acquires the publication without downloading its
block contents. To retain the existing Warden journal and recovery model, it
then mounts one fresh COW upper, reads only the checkpoint metadata blocks,
rebinds the hibernation manifest to destination-local inode/device identities,
prepares the bundle, and adopts the sandbox as parked. Those small local
metadata changes are sealed and durably published as a new snapshot before
the destination reports `staged`; the device and hard-capacity reservation are
then released again. Route CAS therefore never points at an unpublished
destination generation, and neither source nor destination performs a
whole-snapshot transfer.

This deliberately avoids adding a second remote-only parked state to the
Warden journal. Benchmarking must nevertheless reject the choice if metadata
rebinding causes a broad remote read, takes route preparation outside the
latency gates, or creates unacceptable layer-chain growth. Layer compaction
and manifest retention remain bounded background work, never a prerequisite
for route commit.

### 2026-07-30: the storage daemon, not parked route count, owns disk admission

The legacy direct-runtime disk ledger cannot remain the physical admission
authority on a storage-native node: retaining every parked reservation there
would pin durable snapshots to their previous node and recreate fixed bins.
On storage-native nodes it now allocates only stable accounting IDs. The node
storage service is the sole hard-byte authority and fail-closes every create,
resume, and migration staging mount against its configured physical capacity.
An unpublished released layer remains charged; a verified `PUBLISHED` volume
charges zero local authoritative bytes.

Heartbeats report storage hard capacity/reservation, disposable cache bytes,
active and waiting storage operations, publication count, and error count.
Placement uses those daemon metrics plus active CPU/memory and rejects a node
with exhausted hard bytes, a saturated queue, missing storage telemetry, or a
storage reconciliation error. Published parked routes do not reserve node
disk; `WAKING` immediately reserves their complete writable shape until a
heartbeat observes the mounted runtime.

Every ordinary storage-native park publishes before returning its durable
snapshot identity. The gateway stores schema, manifest digest, repository, and
tag with the route and creates a persistent registry reference before marking
the route parked. Migration creates the destination reference before route
CAS and releases the old route reference afterward. This fences registry
prune/GC independently of node survival.

### 2026-07-30: persist portable checkpoint metadata outside the parked device

The first cross-node Warden benchmark review exposed a lifecycle error that
in-memory and fake-filesystem tests could not show: after storage-native park,
the hibernation manifest lives inside the now-unmounted COW device, while
migration preparation tried to read it through the empty mountpoint. Remounting
the device merely to discover a few kilobytes of metadata would add device
work to migration preparation and still leave a parked snapshot dependent on
its original node for recovery.

The Warden therefore mirrors the already-authenticated hibernation manifest
into its node-local control state before it releases the mounted device.
Normal migration preparation reads and verifies that copy against the parked
Warden journal. The copy contains no checkpoint payload and is replaced
atomically on every park/adoption. The durable registry snapshot remains the
filesystem authority; the control copy is only the portable description needed
to verify and rebind its files.

Before production rollout, the ordinary park response and gateway route must
also durably retain the portable descriptor, not only the OCI manifest digest.
That makes a published parked sandbox reconstructable after complete source
node loss rather than merely making its block data durable.

The same review found that overlayfs's `work/` directory was being sealed even
though it is kernel scratch state, not sandbox state, and destination import
correctly rejects it. Warden now removes `work/` only after the merged overlay
has been unmounted and recreates a private empty directory before every local
resume. A parked/importable filesystem therefore contains exactly the durable
`upper/` tree and one authenticated hibernation generation.

### 2026-07-30: the first real cross-node Warden migration passed

The two-node Warden qualification on DFM job `12362112` now exercises the
actual source checkpoint, storage publication, complete source-volume
deletion, destination manifest verification and lazy COW mount, checkpoint
manifest rebinding, destination Warden adoption and republication, restore,
first tool call, and the full stateful conformance workload.

One representative run parked in 163.7 ms, streamed and published the
85,172,224-byte source dense layer in 445.8 ms, acquired and mounted the remote
snapshot at the destination in 49.8 ms, rebound/adopted it in 128.1 ms, and
published the destination delta in 102.7 ms. The destination's new dense layer
was only 2,224,128 bytes, a 97.4% reduction from the source layer; the
87,396,352-byte hybrid commit file length is sparse address-space metadata and
must not be reported as transferred bytes. Destination restore including its
readiness call took 193.8 ms and a separate no-op took 15.8 ms. Source storage
was deleted before destination resume, and the full stateful workload passed.
The whole transaction, including full 16-MiB conformance-state verification,
fetched 21,598,391 bytes in 88 range requests. Raw results are in
`docs/benchmarks/gvisor-storage-native-migration-2026-07-30.json`.

This passes the architectural transfer gate: no whole snapshot is read or
written synchronously at the destination, and destination write amplification
is far below the archive path. Repeated samples and phase-specific range-read
accounting remain required before declaring the latency gates passed. Source
publication is one bounded dirty-layer read and remains separate from route
handoff; the production park API must expose its completion asynchronously
rather than hiding it inside a long node request.

Five consecutive phase-accounted runs subsequently passed. Their p50/p95
latencies were: park 155.1/163.5 ms, source publication 411.3/418.7 ms,
destination acquire/mount 48.9/50.3 ms, destination rebind/adopt 126.5/134.1
ms, destination delta publication 101.4/105.1 ms, complete destination staging
278.0/282.8 ms, restore through readiness 194.2/194.9 ms, and the next no-op
15.8/15.9 ms. Destination dense deltas were 2.07 MiB p50 and 2.11 MiB p95.
Remote bytes through restore and readiness were 5.52 MiB p50 and 5.96 MiB p95;
the next no-op fetched zero additional bytes in all five runs. These samples
pass the representative park, remote-first-tool, warm-tool, and no-whole-read
gates. Raw phase-accounted samples are stored beside the first result under
`docs/benchmarks/`.

### 2026-07-30: retain a strict no-start v2 rollback export

Storage-native nodes now expose an explicit `direct-parked-migration-v2`
export request. It mounts a fresh disposable COW over published authority,
verifies the on-volume manifest against the Warden's private control copy,
constructs destination-local inode/device metadata in memory, and uses the
existing sparse-aware strict archive writer. It never mounts the OCI overlay
and never starts runsc. A new fenced `DiscardMountedCow` storage operation
then destroys the temporary device/upper and returns the journal to the exact
published parent without adding a layer or changing its manifest identity.

The real DFM transaction passed after source deletion and destination
republication. Mount, strict v2 export, archive reinspection, and COW discard
took 124.3 ms for the compressible conformance fixture and produced a
215,955-byte archive. The original storage snapshot then resumed and passed
stateful conformance. Export necessarily reads the checkpoint payload and is
therefore an explicit operator rollback path, never part of normal placement.
Raw results are in
`docs/benchmarks/gvisor-storage-native-rollback-export-2026-07-30.json`.
Existing archive tests reject digest mismatch, unexpected members, unsafe
paths, and invalid metadata; the mounted-source identity and file inventory
are additionally checked before export.

### 2026-07-30: production nodes reserve storage-native bytes outside Docker

The production package treats `direct` as the storage-native runtime, not as a
switch that can silently fall back to the archive-backed direct layout. Its
offline node bundle must contain the exact content-addressed ublk backend
binary emitted by `runtime/storage_native/build_pinned.sh`, that build's
manifest, and its MIT license. Node initialization verifies the bundle
metadata, artifact digest, pinned AgentEnv commit, patch digest, architecture,
and license before installing anything. The backend daemon, UCloud
node-storage service, and direct Warden are separate systemd units ordered in
that sequence; failure of either storage process prevents the node agent from
advertising capacity.

Docker's fixed-size XFS image remains image infrastructure. Authoritative
storage-native layers and writable devices live outside it. Node hard
admission is computed from the VM's physical disk after subtracting the
maximum Docker image size, swap file, bounded disposable block cache, and
explicit filesystem/OS headroom:

```
hard writable capacity =
    physical disk
  - Docker XFS image maximum
  - swap maximum
  - storage cache maximum
  - safety headroom
```

All terms are configured maximums, not current usage, so the writable limit
is usable even when Docker, swap, and cache are full. Storage cache eviction
cannot create capacity and cache bytes are never charged as authority.
Autoscaled direct nodes must retain the VM's actual disk size through init
rather than replacing it with the Docker image size. A non-positive result is
a startup/configuration error, not an overcommit opportunity.

The durable snapshot Registry is reached through the gateway job's stable
private DNS name and a separate repository. An explicitly configured stable
IP may retain a friendly alias, but the default must not pin a gateway VM's
current private IP. Local ublk runtime state and cache remain node-local and
disposable after publication.

### 2026-07-30: the production three-process stack and hard admission passed

On disposable DFM job `12362112`, the release assembly ran the exact pinned
backend, the installed `ucloud-sandboxes-storage` entry point, and
`serve-direct-node-agent` as three independent processes. The node refused to
start while the storage service socket was absent, then became healthy after
the service reconciled. Its heartbeat advertised `storage-native-v1` and
`sandbox-migrate-storage-native-v1`, an exact 4-GiB effective/hard disk pool,
zero reserved/cache/error bytes, and no legacy Docker task owner.

The live metrics check found and fixed an observability error where
`GetMetrics` counted itself as one active operation. Observability requests now
bypass the bounded mutating-operation semaphore, so the idle heartbeat reports
zero active and waiting storage operations and remains available while the
worker queue is saturated.

Hard admission was exercised through the live Unix-socket service: a 3-GiB
volume reservation succeeded, a second 2-GiB reservation was rejected against
the 4-GiB configured capacity, and deletion returned the reservation to zero.
This checks the same fail-closed accounting that production placement consumes.

The release packager rejected the early unpatched compatibility artifact
because its build manifest lacked the streaming-patch identity. A fresh clean
checkout at AgentEnv commit
`f41abb21324f6b0520abf34b7720aa260ddd10eb` passed all 31 patched daemon
protocol tests and emitted provenance-complete backend artifact
`sha256:a2c533d8f86d75013f5e1b25b330317e3c1f3c96bfa9599fdaa23c9b71bd7a7b`.
The final all-in-one release script accepts that artifact and passes shell
syntax validation. Deployment must use that build manifest and its adjacent
binary/license; the earlier `b3ed...` artifact is not releasable.

### 2026-07-31: real SDK/Verifiers migration exposed cold image preparation

Release 0.3.66 deployed successfully on DFM gateway job `12362088`. A real
canonical SDK flow using pinned Verifiers commit
`fcb48822eaf35efe22b6f6deda9c4b422920314e`, its `NullHarness`, the gateway,
relay, and two autoscaled storage-native nodes moved sandbox
`vf-live-7729259458` from job `12362135` to `12362136`. It observed:

```text
running -> parked -> moving_out -> parked -> waking -> running
```

The harness returned `relay-live-park-ok` with exactly one model call, one
trace turn, one durable completion, one expected transport reset, and one
reattachment. The storage descriptor used `storage-native-v1`; no archive
identity or archive transfer was present.

The first cold migration protocol took 25.32 seconds, including 24.78 seconds
in destination staging. This was not accepted as the storage result. The
destination did not create its ublk device until the end of that interval.
The direct import path calls `image_store.materialize()` before storage mount,
while node image prewarming previously stopped after `docker pull`.
Consequently a heartbeat could report the image cached although its immutable
rootfs export had not been created.

An immediate repeat between the same two nodes isolated the true warm path:
the protocol took 1.80 seconds, comprising 74 ms source preparation, 1.08
seconds destination staging, 25 ms route commit, 59 ms destination activation,
and 491 ms source finalization. That is an 8.5x reduction from the previous
15.38-second warm archive protocol, with no whole-snapshot transfer.

Direct-node image warmup must therefore mean both Docker pull and immutable
rootfs materialization. The image-pull API now performs and times both before
returning success; if materialization fails it removes the advertised image
record. This moves cold rootfs CPU/disk work into destination preparation,
before the migration timer and source fence, without moving it onto the
source node or weakening disk admission. A fresh-node production repeat is
required before the drained rollout is complete.

### 2026-07-31: fresh prewarm passed; private IP pinning blocked public qualification

Release 0.3.67 repeated the canonical migration on fresh autoscaled nodes.
Cold destination preparation happened before the source fence, and the
storage-native migration protocol completed in 1.889 seconds: 73 ms source
preparation, 1.297 seconds transfer/staging, 20 ms route commit, 47 ms
activation, and 399 ms source finalization. The 128-second enclosing event was
node provisioning and prewarm outside the migration protocol. This closes the
cold-rootfs regression from 0.3.66.

Gateway job `12362088` then took ownership of the three production public
links, and obsolete job `12361919` was terminated. All public health endpoints
reported 0.3.67. A real public SDK/Verifiers run provisioned worker
`12362141`, but the sandbox relay connection failed before the harness started:
the gateway had moved from `10.36.136.151` to `10.36.144.34` during a
suspend/resume while the autoscaler still installed the old address in worker
`/etc/hosts` and the exact gVisor egress rule. UCloud's job service address
`10.40.63.33` was tested from the worker and was not reachable. The job's
private DNS name resolved to the new address and reached both Relay and
Registry.

The production endpoint rule is therefore:

1. When no explicitly stable IP is configured, Docker, the storage backend,
   and image prewarm use the gateway job's private DNS name directly.
2. The direct-network allowlist accepts that DNS name but installs only exact
   resolved IPv4 `/32` rules above the RFC1918 deny.
3. The node Warden refreshes the mapping every two seconds, installs new rules
   before removing old rules, persists its last successful mapping, and keeps
   that mapping during transient DNS failure.
4. Initial resolution fails node startup closed. A restart never broadens
   sandbox private-network access.

Release 0.3.68 implements this seam without a gVisor patch. Its public
qualification and the follow-up relay fix are recorded below.

The first public 0.3.68 retry clarified the sandbox-facing relay contract.
UCloud's private job DNS name resolves on worker hosts but not inside arbitrary
sandbox images, whose resolver intentionally uses public DNS. Harnesses must
therefore receive the stable public relay origin and its per-rollout
registration token. This is already the SDK default; it survives migration
without rewriting an arbitrary harness's configuration. Private DNS remains
the node-infrastructure path for Registry and storage.

That run moved authority successfully but then returned a false 409 on the
OpenAI client's retry. The durable logical request key and body were identical,
but the request digest included provider-generated `X-Forwarded-*`, `X-Real-IP`,
and `job-id` headers. Those hop-by-hop ingress values changed when the
checkpointed connection was recreated and must neither reach the upstream
worker nor participate in semantic request identity. Release 0.3.69 strips
them at the relay boundary. Regression coverage changes all of those headers
between attempts and requires one enqueue, one worker response, and exact
completed-response replay.

### 2026-07-31: production SDK/Verifiers park, migration, and relay replay passed

Release 0.3.69 is deployed on gateway job `12362088`. The production gateway,
relay, and fallback relay public links all report 0.3.69. The autoscaler is
back at its final homogeneous shape: 2-TB sandbox workers, 440 GB bounded
Docker image storage, 96 GB swap, 32 GB disposable storage cache, 16 GB
headroom, and exact 1x CPU, memory, and disk factors.

The canonical public SDK flow at pinned Verifiers commit
`fcb48822eaf35efe22b6f6deda9c4b422920314e` ran `NullHarness` inside a real
sandbox, using the public relay origin and its scoped rollout token. It parked
on the first model request, provisioned a second full-size node, migrated from
job `12362143` to `12362145`, restored, reattached, and returned
`relay-live-park-ok`. The result had exactly one model call, one trace turn,
one enqueue, one durable completion, one transport reset, one reattachment,
and one wake notification. Relay health from inside the sandbox returned 200.
The final sandbox state was running before the qualification script deleted
it.

The storage-native protocol took 1.212 seconds:

```text
source prepare        29.637 ms
transfer and stage   671.673 ms
route commit          30.144 ms
destination activate  19.935 ms
source finalize      412.432 ms
```

The enclosing migration event took 71.48 seconds because the second 2-TB VM
was provisioned and bootstrapped on demand; none of that time was inside the
source fence or storage protocol. The relay response committed in 1.504
seconds after migration, and the complete harness qualification took 99.87
seconds.

Post-test audit found zero sandbox routes, zero pending demand, zero relay
rollouts/pending requests, and empty inventories on both qualification
workers. Jobs `12362143` and `12362145` were then explicitly terminated and
reached `SUCCESS`. Obsolete gateway job `12361919` was already terminal. The
scoped local gateway/relay test credentials were deleted. This completes the
production rollout gate; future work may optimize destination provisioning,
but it is separate from the qualified 1.212-second migration protocol.
