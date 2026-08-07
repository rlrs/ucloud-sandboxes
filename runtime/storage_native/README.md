# Storage-native backend

This directory builds and qualifies the node-wide ublk/OverlayBD storage
dependency used by the storage-native runtime. AgentEnv supplies the block
device implementation; the direct Warden remains the sole sandbox lifecycle
owner.

The dependency is pinned to AgentEnv commit
`f41abb21324f6b0520abf34b7720aa260ddd10eb` under its MIT license. Build it
from an exact, clean checkout:

```bash
./build_pinned.sh /path/to/AgentENV /path/to/artifacts
```

The build applies the dense-stream export, pooled-exclusive-delete, and
owner-identity patches, runs the daemon protocol tests, and emits a
content-addressed binary, license, and schema-3 build manifest with every patch
digest. A production package must use that manifest and must not fetch or build
an unpinned branch during node startup.

The pooled-delete patch is deliberately narrow. AgentEnv continues to own pool
acquire, target swapping, cache eviction, refill, and release. The patch only
allows an explicit `Delete` request to permanently destroy an active exclusive
pooled device after uncertain mount cleanup; it refuses shared pooled devices.
The owner-identity patch makes runtime-device acquisition idempotent and
reports active owner bindings separately from idle devices, allowing the
UCloud journal to recover an acquisition interrupted before the device id was
recorded.

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
