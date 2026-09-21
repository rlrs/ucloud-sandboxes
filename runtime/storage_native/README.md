# Storage-native backend

This directory builds and qualifies the node-wide ublk/OverlayBD storage
dependency used by the storage-native runtime. AgentEnv supplies the block
device implementation; the direct Warden remains the sole sandbox lifecycle
owner.

The dependency is pinned to the AgentEnv v0.1.2 release commit
`db1492b7915a408b37f863c9e3a34b2ccb2fb1b0` under its MIT license. This release
contains the compact-index ordering, oversized-segment, shared remote-cache,
and adaptive warm-pool fixes used by detached publication and wake. Build it
from an exact, clean checkout:

```bash
./build_pinned.sh /path/to/AgentENV /path/to/artifacts
```

The build applies the dense/compacted-stream export, pooled-exclusive-delete,
owner-identity, owner-transition, premerged-identity, and device-reuse patches.
It runs targeted compaction, cache, ownership, device-pool, and protocol tests and emits a
content-addressed binary, license, and schema-3 build manifest with every patch
digest. A production package must use that manifest and must not fetch or build
an unpinned branch during node startup.

AgentEnv v0.1.2 requires both `--global-config` and
`--resize-global-config`. Production init writes separate runtime and resize
configs backed by sibling `remote-blocks` and `resize-blocks` directories.
They must remain isolated: the offline C++ resize cache has destructive
eviction semantics that are not compatible with the shared Rust runtime cache.
Background download remains disabled in both configs.

The device-reuse patch treats the pool high watermark as a steady-state idle
cache target, not a bound on active devices or recently returned devices.
Returns stay reusable for 60 seconds; a five-second maintenance task retires
only expired surplus. Acquisitions prefer exact-size devices before resizing.
Prewarming maintains the low watermark and defers to concurrent returns;
allocation failures retain the single-flight guard during exponential backoff
from one to 32 seconds. Shutdown rejects late returns. Mount ownership,
exclusive-use checks, and quarantine remain prerequisites for safe reuse.

Sandbox VM init also sets `vm.watermark_scale_factor=100` so background reclaim
starts with more free-page headroom during buffered snapshot/rootfs I/O. This
does not cap sandbox memory or disable compaction. The isolated UCloud
[qualification](../../docs/reviews/device-reuse-memory-2026-09-20.md) records
the measured reduction in churn and compaction, including its limitations.

## Published snapshot-chain compaction

Each ordinary publication appends the newly sealed writable layer to the
existing immutable Registry chain. The node compacts the prospective chain
before publishing when it would exceed either eight layers or 4 GiB of accumulated
delta data after the oldest base layer. Local sealed layers use allocated bytes
as the size estimate, excluding sparse virtual-address holes. Excluding the base
avoids repeatedly flattening an already compacted multi-GiB snapshot after every
small update. These are compaction triggers, not snapshot size limits. They bound
lookup depth and Registry metadata without putting a
large temporary flattened file on the worker's constrained local disk.

When only depth triggers maintenance and the published base is larger than all
newer layers combined, the publisher retains that immutable base and merges only
the newer layers into one delta. The result has two layers and avoids reading or
uploading the base. This applies with a depth threshold of at least two; an
explicit one-layer threshold still forces a full merge. Accumulated delta bytes
above the byte threshold, a non-dominant base, or a blob-origin change triggers a
full merge. Thus delta growth eventually reclaims obsolete base data.

Compaction opens the selected remote-plus-local layers through AgentEnv's shared
bounded cache, flattens them with ordered writes, and streams one content-addressed
layer directly into the durable backend. Partial merges must preserve explicit
zero/discard mappings and unmapped holes, since an older base remains underneath. Publication is
transactional at the control-plane boundary: the old Registry descriptor and
local sealed delta remain authoritative until the new OCI manifest is durable.
An export, upload, or manifest failure therefore leaves a resumable attached
park rather than deleting its last valid state. Registry garbage collection can
later reclaim any unreferenced upload created by a failed attempt.

Force the threshold to one layer to exercise compaction in the real node
service qualifier:

```bash
sudo python3 benchmark_node_service.py \
  --daemon /path/to/uvm-ublk-daemon-<sha256> \
  --work-root /var/lib/ucloud/storage-native-qualification \
  --output /tmp/storage-native-compaction.json \
  --registry-url http://127.0.0.1:18080 --repository snapshots \
  --compact-after-layers 1 --compact-after-bytes 4294967296 \
  --enable-pool --pool-low-watermark 2 --pool-high-watermark 4
```

The Hetzner CPX62 qualification compacted two generations into one 98.7 MB
layer in 0.868 seconds, then mounted and verified both generations in 59 ms.
That is a correctness and small-warm-chain measurement, not a cold multi-GiB
compaction or concurrent-publication SLO. Machine-readable evidence is in
`docs/benchmarks/hetzner-agentenv-compaction-2026-08-12.json`.

The pooled-delete patch is deliberately narrow. AgentEnv continues to own pool
acquire, target swapping, cache eviction, refill, and release. The patch only
allows an explicit `Delete` request to permanently destroy an active exclusive
pooled device after uncertain mount cleanup; it refuses shared pooled devices.
The owner-identity patch makes runtime-device acquisition idempotent and
reports active owner bindings separately from idle devices, allowing the
UCloud journal to recover an acquisition interrupted before the device id was
recorded. The owner-transition patch makes the forward/reverse ownership index
atomic, fences late release/delete completions to their captured owner, and
serializes retries only for the same owner. Registry locks never span device
I/O; unrelated acquisitions remain concurrent. Its regressions reproduce a
release completing after the pool has reassigned the same numeric device ID.

## Destructive volume qualification

Run only as root on a disposable Linux VM with kernel ublk support. The
qualifier creates and formats a ublk device, mounts filesystems and overlayfs,
freezes and seals the device, destroys it, reconstructs a new device from the
sealed layer, verifies filesystem state, and checks the requested hard device
boundary:

```bash
sudo python3 qualify_volume.py \
  --daemon /path/to/uvm-ublk-daemon-<sha256> \
  --work-root /var/lib/ucloud/storage-native-qualification \
  --output /tmp/storage-native-volume.json
```

Required host tools are `mkfs.ext4`, `mount`, `umount`, `fsfreeze`,
`fallocate`, `setfacl`, and `getfacl`. The work root must be an existing,
dedicated directory; the qualifier creates a unique child and never removes
the supplied root.

Pass `--runsc`, `--conformance-workload`, and `--noop-workload` together to
run the pinned gVisor workload as part of the same destroy/reconstruct cycle.

Compare steady-state XFS behavior against a same-disk native loopback XFS:

```bash
sudo python3 benchmark_io.py \
  --daemon /path/to/uvm-ublk-daemon-<sha256> \
  --work-root /var/lib/ucloud/storage-native-qualification \
  --output /tmp/storage-native-io.json
```

The benchmark alternates target order across rounds and applies a 15% gate to
sequential write bandwidth, 70/30 random mixed IOPS, and a
create/stat/rename/delete metadata workload.

## Warm-device churn benchmark

`benchmark_node_service.py` can compare the real journaled XFS lifecycle with
and without AgentEnv's warm block-device pool:

```bash
sudo python3 benchmark_node_service.py \
  --daemon /path/to/uvm-ublk-daemon-<sha256> \
  --work-root /var/lib/ucloud/storage-native-qualification \
  --output /tmp/storage-native-unpooled.json \
  --churn-iterations 100 --parallel-volumes 8 --parallel-rounds 10

sudo python3 benchmark_node_service.py \
  --daemon /path/to/uvm-ublk-daemon-<sha256> \
  --work-root /var/lib/ucloud/storage-native-qualification \
  --output /tmp/storage-native-pooled.json \
  --churn-iterations 100 --parallel-volumes 8 --parallel-rounds 10 \
  --enable-pool --pool-low-watermark 2 --pool-high-watermark 16
```

The benchmark deletes every test volume and fails if any hard reservation
remains. Raw DFM results are stored in `docs/benchmarks`; the 2/16 pool reduced
sequential wake-plus-release p50 by about 38%, reduced eight-way release p50 by
about 65%, and reused a warm device for all 189 measured acquisitions. The
stock high watermark of 64 eagerly created 64 idle devices and provided no
eight-way throughput benefit, so production defaults to 16.

## Delta-compaction qualification

On an idle Linux qualification worker with the pinned backend already running:

```bash
sudo env PYTHONPATH=/path/to/repository python3 benchmark_delta_compaction.py \
  --backend-socket /run/ucloud-sandboxes/storage-native/backend.sock
```

This compares full and delta-only native exports for a 128-MiB base and eight
small deltas. It checks repeated overwrites, explicit zero writes, discards,
holes, and native restacking of the retained base plus the merged delta. It uses
temporary local layers and export RPCs only, with no device creation, mount,
production journal changes, or provider lifecycle operations. Results exclude
network upload and concurrent application traffic.
