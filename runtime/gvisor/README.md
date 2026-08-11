# Pinned gVisor runtime

Sandbox nodes run the deployment-pinned `runsc` binary directly under the
privileged Warden. Docker and containerd provide OCI image layers only; they do
not own sandbox processes, writable storage, or lifecycle state.

The five patches in this directory add the disk-backed application memory and
transactional park/wake primitives required by the Warden:

1. external application-memory backing;
2. quota-owned memory directories;
3. two-phase hibernation capture;
4. bounded restore CPU startup burst;
5. paused restore handoff.

The source is pinned to gVisor `release-20260721.0`, commit
`9f653e577965df2ddd13875b5530cd2588661f1c`. `build_pinned.sh` verifies that
source identity and the patch digests before producing a content-addressed
binary and build manifest:

```bash
./build_pinned.sh /path/to/gvisor /path/to/artifacts
```

The resulting `runsc` artifact is part of the verified sandbox-node bootstrap
bundle. A node never substitutes a repository package or downloads a runtime at
boot.

## Image root filesystems

`DockerOverlay2RootfsStore` is the only image-rootfs implementation. It mounts
Docker's immutable overlay2 layers without flattening or exporting the image,
and pins every referenced image by digest so pruning cannot remove layers below
a live or parked sandbox. Startup calls `reconcile_images()` to recover the
mounted image set from durable metadata before admitting work.

The canonical production measurement is
[`rootfs-overlay2-production-2026-08-02.json`](../../docs/benchmarks/rootfs-overlay2-production-2026-08-02.json).

## Runtime verification

The focused runtime tests are:

```bash
bazel test //pkg/sentry/pgalloc:pgalloc_test //runsc/boot:boot_test \
  //runsc/cmd:cmd_test
```

The repository benchmarks exercise the current direct-Warden boundary:

- `benchmark_disk_memory.py`: disk-backed application-memory behavior;
- `benchmark_hibernate.py`: park/wake latency and content verification;
- [`../storage_native/benchmark_warden.py`](../storage_native/benchmark_warden.py):
  storage-native Warden lifecycle, wake-burst, and density qualification;
- `benchmark_direct_node_api.py`: product-facing API lifecycle;
- `benchmark_crash_recovery.py`: durable recovery;
- `benchmark_fsync.py`: artifact publication cost.

`qualify_direct_node.py` verifies create, exec, file transfer, park, wake,
delete, and daemon-restart recovery through the node API.

## Ownership invariants

- The Warden is the sole writer for each sandbox generation.
- XFS project quota is established before application memory is created.
- Park publication is durable before the live process is reaped.
- Wake records the paused candidate before guest execution resumes.
- Failure leaves one recoverable owner; it never creates two writable owners.
- OCI rootfs layers stay immutable. Writable state belongs to the
  storage-native service.
