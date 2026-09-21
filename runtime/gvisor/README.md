# Pinned gVisor runtime

Sandbox nodes run the deployment-pinned `runsc` binary directly under the
privileged Warden. Docker and containerd provide OCI image layers only; they do
not own sandbox processes, writable storage, or lifecycle state.

The first patch, `20260817/0001-ucloud-hibernation.patch`, ports all five
Warden primitives to gVisor release `20260817.0`, commit
`50e1502a95d36ad2faf2c7ef33b8bf21fe975293`:

1. external application-memory backing;
2. quota-owned memory directories;
3. two-phase hibernation capture;
4. bounded restore CPU startup burst;
5. paused restore handoff.

The second patch, `20260817/0002-ucloud-default-acl-umask.patch`, fixes upstream
[issue 13688](https://github.com/google/gvisor/issues/13688). Creation carries
the requested mode and umask separately through VFS and overlay. Tmpfs applies
the umask when the final parent has no default ACL; inherited ACL permissions
are instead bounded by the original requested mode. Explicit private modes
remain private, and filesystems without ACL inheritance retain ordinary umask
behavior. Unix socket bind retains Linux's separate rule of applying umask
before creating the socket, including under a default ACL.

The third patch, `20260817/0003-ucloud-release-detached-mounts.patch`, opens
the sentry executable in the host mount namespace before spawning it. Direct
and prewarmed launches execute through that descriptor, then close the donation.
It also disables Go's automatic cgroup CPU watcher in sentry and gofer children,
while preserving gVisor's explicit CPU sizing and the OCI CPU quota. The sentry
starts with two Go scheduler processors until its loader applies the configured
CPU count. These changes prevent executable mappings and cached `cpu.max`
descriptors from retaining detached cloned mount trees and sibling sandbox disks.
Live density qualification is required before rollout.

The original five July patches remain here as historical reference. They are
not applied by the current build. All three August patches are applied and attested.
The port uses upstream's new protobuf memory
metadata, checks external backing size before installing allocator state, and
patches the sentry's new `runsc/cmd/sentry/sentrycmd/boot.go` location.

`build_pinned.sh` verifies the exact source, patch, patched file contents and
Bazel version. It runs the focused patch tests and builds upstream's complete
release target. Use a clean checkout at the pinned commit on Linux:

```bash
sudo ./build_pinned.sh /path/to/gvisor /path/to/artifacts
```

The container tests require root or working unprivileged user namespaces.
The UCloud Ubuntu 26.04 build used Bazel 8.3.1, `build-essential`,
`gcc-aarch64-linux-gnu`, `g++-aarch64-linux-gnu`, `clang`, `llvm`, `libbpf-dev`,
and `libc6-dev-i386`. Put source and Bazel caches on local disk with enough
space; UCloud's `/tmp` is a memory-backed filesystem.

The output is a content-addressed directory containing `runsc`,
`build-manifest.json`, and four executable companions in `gvisor-bin/`.
Pass that directory's `runsc` to deployment's existing `--direct-runsc` option
and set `sandbox.direct_runsc_commit` to the source commit above. Keep the whole
directory together: the CLI validates and stages all companions, the bundle
builder verifies them again, and node bootstrap verifies and installs the exact
set under `/usr/local/libexec/ucloud-gvisor/`. Nodes do not download runtimes or
companions during startup. Each manifest records every executable's size and
SHA256; independent builds get separate directories rather than sharing sidecars.
The node also includes the installed companion hashes in checkpoint compatibility
fingerprints, so changing only the sentry cannot silently reuse an old checkpoint.

**Checkpoint migration:** July and August checkpoints are not interchangeable.
The allocator wire format changed. Preserve the old executable distribution for
old sandboxes, and drain/delete or explicitly migrate workload data before
replacing their runtime. Do not relabel old checkpoint fingerprints or attempt
to restore old memory images using this release. Legacy deployment bundles
remain accepted with their original explicit runtime commit.

The August distribution with the ACL patch also requires a fresh runtime
generation. The added VFS option fields change generated save/restore layouts;
do not assume compatibility with checkpoints from the earlier August build.
The exact executable and companion fingerprints enforce this boundary even
though the upstream source commit is unchanged.

Actual UCloud qualification and artifact identity are recorded in
[`gvisor-integration-2026-09-05.md`](../../docs/reviews/gvisor-integration-2026-09-05.md).

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
  //runsc/cmd:cmd_test //pkg/sentry/fsimpl/tmpfs:tmpfs_test
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

`compatibility_workload.py` additionally verifies cross-user writes with
default ACLs and umasks 0022/0077 on `/tmp` and `/srv`, including nested
directories, FIFOs, Unix sockets, setgid ownership, explicit 0600/0700 modes,
and controls without default ACLs. `qualify_gvisor_hibernation.py` requires
this matrix to pass before and after hibernation. These are qualification
requirements; adding the patch does not itself establish a live runtime pass.

## Ownership invariants

- The Warden is the sole writer for each sandbox generation.
- XFS project quota is established before application memory is created.
- Park publication is durable before the live process is reaped.
- Wake records the paused candidate before guest execution resumes.
- Failure leaves one recoverable owner; it never creates two writable owners.
- OCI rootfs layers stay immutable. Writable state belongs to the
  storage-native service.
