# Experimental gVisor disk memory and local hibernation

This directory contains the reproducible artifacts for the phase-3 experiments
in [`docs/gvisor-parking-plan.md`](../../docs/gvisor-parking-plan.md). The work
is pinned to gVisor `release-20260721.0`, commit
`9f653e577965df2ddd13875b5530cd2588661f1c`.

The patch keeps upstream memfd/checkpoint behavior as the default and adds:

```text
--application-memory-file-dir=/absolute/host/path
runsc checkpoint --hibernate --image-path=... CONTAINER
runsc restore --cpu-startup-burst ...
```

The runsc parent creates a private sparse regular file and donates it as the
main `pgalloc.MemoryFile`. Hibernation serializes allocator metadata and the
kernel object graph without scanning or copying application pages, leaves guest
tasks and internal time updates quiesced, and renames the canonical backing file
into the checkpoint directory. The sentry deliberately remains alive: the node
Warden can resume it if durable publication fails, or stop it only after the
artifact and `COMPLETE` marker are durable. Restore creates a new sentry, maps
that same inode, and moves it back into active storage.

The current artifact is intentionally narrow:

- hibernate and active-memory directories must be on the same filesystem;
- restore consumes the checkpoint's application-memory file, so it is
  single-owner and cannot be used for fork/fan-out;
- `runsc` captures a locally consistent generation but the node Warden, not the
  runtime, owns fsync, manifest publication, exact-process termination, and
  crash reconciliation;
- only the main application `MemoryFile` uses external backing; private
  `MemoryFile`s still use the ordinary pages file;
- active memory is created with dirfd-relative `openat(O_NOFOLLOW)` in a
  pre-created per-incarnation directory selected by an exact OCI annotation;
- the XFS helper sets project ownership, inheritance, and a hard limit before
  runsc creation; the separate v1 manifest/journal is not yet invoked by
  runsc itself;
- the complete configured memory limit must be physically reservable on disk.

The restore-only CPU startup burst is transactional. A newly restored candidate
starts without its steady-state OCI CPU quota, then `runsc` reapplies the exact
resources before the restore command succeeds. The Warden admits no exec until
that boundary. This avoids multiplying wake latency for small CPU requests
while preserving the requested quota for all sandbox work.

These constraints are deliberate. OverlayBD, ublk, layering, dirty-page
tracking, and reflink lineage are only needed for cloning, remote storage, or
crash-durable generations; they are not needed to test whether linear
hibernate/restore can eliminate page copies and stopped runsc processes.

## Build and tests

The preferred path verifies the exact source and patch, runs the four tests
added by the patch series, builds with `-c opt`, and emits a content-addressed
binary plus build manifest. Filtering to the patch tests avoids making the
artifact build depend on unrelated host-network tests in gVisor's much larger
target:

```bash
./build_pinned.sh /path/to/gvisor /path/to/artifacts
```

The script is idempotent for an already-patched checkout but refuses another
source commit, patch digest, or unrelated dirty tree. For manual development,
apply the four numbered patches in order to the pinned checkout, then:

```bash
bazel build -c opt //runsc:runsc
bazel test //pkg/sentry/pgalloc:pgalloc_test //runsc/boot:boot_test \
  //runsc/cmd:cmd_test
```

On Ubuntu 24.04, the build host must also have the native toolchain and the
AArch64 C/C++ cross compilers (`build-essential`, `gcc-aarch64-linux-gnu`, and
`g++-aarch64-linux-gnu`) plus `clang`, `libbpf-dev`, and `libc6-dev-i386` for
the eBPF support objects. Install `libc6-dev-i386` before the AArch64 compiler
packages on Ubuntu because its temporary `gcc-multilib` dependency conflicts
with the cross-compiler metapackage. gVisor builds architecture-specific
support objects even when the emitted `runsc` binary is x86-64.

The allocator tests prove metadata-only round-trip content and reject an
incorrect backing-file size without mutating the destination allocator.

## Disk pageout benchmark

Compile the stateful workload statically and run:

```bash
gcc -O2 -static -Wall -Wextra -Werror \
  -o /tmp/memory-workload memory_workload.c
sudo python3 benchmark_disk_memory.py \
  --workload /tmp/memory-workload \
  --output gvisor-disk-memory.json
```

This compares stock memfd plus swap with the regular-file main memory while
the sentry remains alive. It measures writeback, reclaim, lightweight exec,
and checksum-verified dense and sparse scans.

## Hibernation benchmark

Prepare an OCI bundle whose rootfs contains `/memory-workload`, then run the
custom binary directly:

```bash
sudo python3 benchmark_hibernate.py \
  --runsc /usr/local/bin/runsc-hibernate \
  --bundle /tmp/hibernate-bench/bundle \
  --root /tmp/hibernate-bench/runroot \
  --memory-dir /tmp/hibernate-bench/memory \
  --checkpoint-root /tmp/hibernate-checkpoints \
  --output gvisor-hibernate.json
```

The default matrix is dense 256 MiB, 1 GiB, and 4 GiB plus a sparse 4 GiB
mapping with 1 MiB populated. It performs three hibernate/restore cycles. The
middle cycle calls `sync` and drops host page cache on the dedicated benchmark
VM, so restore latency and deferred cold page-in are reported separately.
Every cycle checks the in-memory sentinel before scanning all populated pages.

Raw results from the UCloud run are in
[`docs/benchmarks/gvisor-hibernate-2026-07-27.json`](../../docs/benchmarks/gvisor-hibernate-2026-07-27.json).

The direct-runsc lifecycle does not imply Docker compatibility. The
Docker/containerd gate is documented in
[`docs/benchmarks/gvisor-docker-gofer-2026-07-28.json`](../../docs/benchmarks/gvisor-docker-gofer-2026-07-28.json):
containerd treats the reaped sentry as task exit, tears down the shim/gofer and
bundle, and `docker start` boots fresh state. Stock Docker therefore cannot own
the current hibernation lifecycle.

## Direct node Warden benchmarks

The selected production boundary is one privileged node Warden owning every
direct-runsc backend. Docker resolves and exports images but never starts a
sandbox task. The relevant entry points are:

```bash
sudo python3 benchmark_image_rootfs.py ...
sudo python3 benchmark_warden.py ...
sudo python3 benchmark_direct_node.py ...
sudo python3 benchmark_direct_density.py ...
```

`benchmark_direct_node.py` creates independent overlay-backed sandboxes and
measures bounded, tool-triggered restore bursts. With
`--conformance-workload`, every exec validates a 16 MiB mapping, processes,
threads, pipes, sockets, epoll/eventfd, timerfd, futex/condition state,
deleted-open files, and signals. `benchmark_direct_density.py` measures live
sentry RSS and parked artifact allocation for the same stateful workload.

`qualify_direct_node.py` exercises the product-facing HTTP boundary rather than
the Warden classes directly. Its `prepare-restart` and `resume-after-restart`
phases bracket a daemon restart and verify image materialization, create,
streaming exec, binary file transfer, park, durable reconciliation, wake, and
delete. `benchmark_direct_node_api.py` drives the same deployed API with a
bounded concurrent create/park/wake/delete burst and verifies per-sandbox state
after every restore.

The 2026-07-28 UCloud results are:

- 100 Warden park/restore cycles:
  [`gvisor-warden-two-phase-2026-07-28.json`](../../docs/benchmarks/gvisor-warden-two-phase-2026-07-28.json);
- Docker image export and overlay setup:
  [`gvisor-image-rootfs-2026-07-28.json`](../../docs/benchmarks/gvisor-image-rootfs-2026-07-28.json);
- 24-way stateful bounded wakes:
  [`gvisor-direct-node-stateful-burst-2026-07-28.json`](../../docs/benchmarks/gvisor-direct-node-stateful-burst-2026-07-28.json);
- 256-sandbox stateful density:
  [`gvisor-direct-density-256-2026-07-28.json`](../../docs/benchmarks/gvisor-direct-density-256-2026-07-28.json).
- clean-node product-facing API qualification:
  [`gvisor-direct-node-api-2026-07-28.json`](../../docs/benchmarks/gvisor-direct-node-api-2026-07-28.json).
- CPU-quota-matched product API burst:
  [`gvisor-direct-node-api-cpu-startup-burst-2026-07-28.json`](../../docs/benchmarks/gvisor-direct-node-api-cpu-startup-burst-2026-07-28.json).

The clean-node deployment record, remaining release gates, and legacy removal
sequence are in
[`direct-runtime-production-qualification.md`](../../docs/direct-runtime-production-qualification.md).

## Durable seal benchmark

The metadata-only benchmark stops after the memory inode is renamed. Production
must also make every artifact durable before publishing `COMPLETE`. Measure the
missing worst-case cost independently with:

```bash
sudo python3 benchmark_fsync.py \
  --root /var/lib/ucloud-sandboxes/hibernate-fsync \
  --output gvisor-hibernate-fsync.json
```

The benchmark dirties one byte in every 4 KiB page of each sparse file and
reports the subsequent `mmap.flush()` plus `fsync()` latency. This is a
worst-case newly-dirty seal, not a claim that every park rewrites the full
configured memory.
