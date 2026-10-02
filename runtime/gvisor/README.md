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

The fourth patch adds capture-abort recovery. The fifth fixes root EROFS
filestore FD donation; the host EROFS adapter remains an optional image backend.
The sixth adds RAM-active application memory with sparse export and restore
population inside the candidate's cgroup. The seventh adds
`--application-memory-reflink-restore`: file-backed restore creates a private
XFS reflink before installing the allocator, so it does not eagerly copy the
entire application heap or consume the immutable checkpoint. RAM and reflink
flags are mutually exclusive for an individual runtime invocation. The Warden
persists each allocation's backing mode and owns the transition at parked
restore; runtime flags do not create a second lifecycle authority.

The clone path requires a same-filesystem, regular, immutable source and an
exclusively created target. It has no eager-copy fallback. Failure leaves the
complete source intact; successful restore installs the candidate as the active
file, and only the Warden's durable RUNNING handoff permits source retirement.
The source occupies its own exact-quota retention project, admitted through the
existing physical-capacity ledger before cloning. Sharing extents is not treated
as free quota or permission to overcommit physical disk.

The native and product Warden qualification is recorded in
[`memory-tiers-2026-09-23`](../../docs/benchmarks/memory-tiers-2026-09-23/README.md).
It includes incomplete restore, private-clone integrity, real project transfer,
RAM-to-file ownership, live reclaim, TCP and SQLite checks. Density and provider
performance remain separate rollout gates.

All seven August-series patches are applied and attested.
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

Workers with `immutable_environments.worker_enabled`, including the Hetzner
production workers, use the EROFS adapter `EnvironmentRootfsStore`, which reads
image chunks on demand over NBD; see
[`immutable-environments.md`](../../docs/immutable-environments.md).

Without `immutable_environments`, `DockerOverlay2RootfsStore` is the code
default. It mounts Docker's immutable overlay2 layers without flattening or
exporting the image, and pins every referenced image by digest so pruning cannot
remove layers below a live or parked sandbox. Startup calls `reconcile_images()`
to recover the mounted image set from durable metadata before admitting work.

The canonical overlay2 production measurement is
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

## RL-scale spikes

`spike_rl_scale.py` answers spikes S1, S2, S4, S6, S7 and S8 of the
[RL-scale plan](../../docs/rl-scale-architecture-plan.md) (section 9) against
the pinned runsc. S3 (multi-lower EROFS) and S5 (restore into another netns)
need runtime changes and are not covered. Run it as root on a disposable
qualification VM, never on a node that serves sandboxes:

```bash
sudo python3 runtime/gvisor/spike_rl_scale.py \
  --runsc /usr/local/bin/runsc --work-root /srv/rl-spike \
  --output rl-spike-$(hostname)-$(date +%Y%m%dT%H%M).json \
  --rootfs /srv/images/python-rootfs \
  --variant gofer,gofer-shared,erofs --erofs-image /srv/images/python.erofs
```

- `--work-root` must be an existing root-owned directory that is not group- or
  world-writable, and either empty or previously used by this script. Each run
  works in `run-<id>/` below it and in its own cgroup
  `/sys/fs/cgroup/ucloud-rl-spike-<id>`. Container cgroups are pre-created
  there so runsc never writes to an ancestor's `cgroup.subtree_control`.
- `--probe s1,s7` selects probes (default: all). S2 needs `--rootfs` (an
  unpacked image) for the `gofer` and `gofer-shared` variants and
  `--erofs-image` for `erofs`. Build the image as `qualify_erofs.py` does:
  `mkfs.erofs -T0 -E noinline_data`.
- S2 starts K ∈ `--k` (default 1,8,32) sandboxes per variant and runs
  `--read-command` in each. It reports host `MemAvailable`, per-sandbox cgroup
  `memory.current` and `memory.stat`, Sentry RSS/PSS/USS from `smaps_rollup`,
  and memory-file allocation. The ratio is host bytes for K / (K × bytes for 1):
  1.0 means duplicated per Sentry. `scaling_vs_k1` is the plan's ≤ 1.2 gate.
  `gofer-shared` also passes `--overlay2=none`, because runsc rejects
  `--file-access=shared` together with a root overlay.
- S1 and S4 use a static busybox (`--busybox`, default `/usr/bin/busybox`).
- S6 always probes `memory.reclaim swappiness=` on an empty test cgroup. The
  freeze-and-reclaim swap test runs only with `--allow-swap-test` and an active
  swap device.

Status `pass` means the answer is the one the plan assumes; S2 reports its
duplication verdict per variant in `answer`. `fail` is a measured contrary
answer, `unsupported` an absent feature, and `error` a probe that could not
complete. Every section lists the exact commands it ran. On a cleanup failure
the run directory is kept and named in `retained_run_dir`. The script never
overwrites an existing report.

The client-side metrics (time to first command, burst, creation rate, density
at p99, park/wake) come from `scripts/bench_rl_scale.py`. Its reports point to
this probe for page sharing and PSS/USS.
