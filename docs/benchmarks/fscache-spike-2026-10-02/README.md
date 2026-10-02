# S11: Nydus RAFS v6 over EROFS fscache on-demand mode (2026-10-02)

Spike S11 for plan item C2.13 (`docs/rl-scale-architecture-plan.md`). The question:
should workers serve Nydus RAFS v6 images through kernel EROFS over fscache on-demand
mode, with `nydusd` as the on-demand daemon, instead of our NBD block path?

Our Ubuntu 26.04 kernels do not enable on-demand mode
(`CONFIG_EROFS_FS_ONDEMAND` and `CONFIG_CACHEFILES_ONDEMAND` are unset). The spike
therefore also tests rebuilding those two modules for the pinned worker kernel
`7.0.0-30-generic`.

## Verdict

**Keep NBD as the C2.13 serving path. Do not adopt fscache now.**

**Measured result.** On the same images, fscache was clearly faster than today's path:
- 20 sandboxes cold-starting in parallel finished in 4.0 s instead of 8.3 s;
- the host used 38 CPU-seconds instead of 68;
- 245 MB was fetched instead of 380 MB;
- `pip --version` on a cold cache took 1.95 s instead of 3.45 s.

It also passed every correctness check: file contents, chunk digest validation, and
recovery across a daemon crash when a supervisor holds the device fd.

**Why it is still not the path.** The risks fall on the kernel and operations side:
- **Deprecated upstream.** In this kernel the option reads "EROFS fscache-based on-demand
  read support (deprecated)". Its Kconfig help says it is "scheduled to be removed from
  the kernel after fanotify pre-content hooks are landed". Every mount logs
  `[deprecated] fscache-based on-demand read feature in use. Use at your own risk!`
- **Unsigned out-of-tree modules.** Canonical neither builds nor tests the option.
  Our modules taint the kernel (`O`+`E`). They load only because Hetzner VMs have no
  Secure Boot and no lockdown.
- **Operational gaps.** A plain `nydusd` restart breaks every mount (`ENOBUFS`), so
  production needs a supervisor. Eviction is per whole blob, with no byte budget.
  `nydusd` returns `EAGAIN` on about 1 in 80 parallel binds, and an unimplemented API
  call panics its API thread.

**Where the speed comes from.** Most of the measured advantage is not fscache itself:
- zstd-compressed transfer, which fetches 35–50% fewer bytes;
- 1 MiB fetch units, against our 256 KiB;
- a native daemon;
- concurrent attach: our backend serializes attaches, so attach waited 1.6 s at the
  median under a 20-way burst, against 0.25 s.

All of these can be built into our NBD backend, and S10's multi-device NBD path should
be measured against these numbers.

**Revisit fscache only if all of these hold** (see Recommendation):
- upstream keeps the feature, or names a successor we can adopt;
- the S10 NBD path cannot get within about 1.3× of these numbers;
- we accept carrying signed out-of-tree modules for each kernel bump.

## Method

**Host.** One disposable Hetzner VM, `sandboxes-spike-fscache`, from the 0.8.1 worker
snapshot `438710747` (Ubuntu 26.04.1, kernel `7.0.0-30-generic`).
- The requested `cpx62` was refused (`resource_limit_exceeded`: shared core limit;
  S10's `cpx62` was using the shared-core quota). The spike used a **ccx43** instead: 16 dedicated
  vCPU, 64 GB RAM, 360 GB disk.
- VM init was never run. S10's VM and files were not touched.
- Runtime: created 11:52:19Z, deleted 12:58:02Z, so **1 h 06 min (about 1.1 VM-hours)**.
  Deletion was confirmed through the Hetzner API and the resource ledger.

**Inputs.**
- **Images:** 20 prepared references from
  `all-cached-training-tasks-with-terminal-lego-2026-10-01.zip`:
  - 8 ScaleSWE complete images: 4 `getsentry/responses`, 2 `oauthlib`, 2 `traitlets`;
  - 6 TMax: the shared source image used by 3,649 rows, plus 5 foundations;
  - 6 Terminal-Lego: 2 shared sources, plus 4 foundations.

  The list is in [raw/images.json](raw/images.json).
- **Registry:** read-only from production `http://10.42.0.2:5000`. Nothing was written to
  production.
- **Node bundle:** `release-0.8.1-20261002/sandbox-node-package.tar.gz` from the gateway,
  which supplies `runsc` (gVisor `release-20260817.0`, our patched build) and the 0.8.1
  agent runtime.
- **Nydus:** v2.4.5 static release (`nydusd`, `nydus-image`, `nydusify`). The registries
  are `distribution` v3.0.0.

**Two local registries on the VM.** Both paths read from loopback, so neither has a WAN
advantage.
- `:5001` holds the converted Nydus images.
- `:5002` is a pull-through cache of production for our EROFS components. It was
  pre-warmed with every component blob first (8.8 GB).
- Bytes fetched are nftables counters on each registry's source port. They were
  validated against curl and against both daemons' own counters (within 1%;
  [raw/counter-check.json](raw/counter-check.json),
  [raw/bytes-check2.json](raw/bytes-check2.json)).

**Today's path, as run here.**
- Our real `EnvironmentBackend` (`serve-environment-io`, with 0.8.1's
  `PrefetchPolicy(enabled=True)` and a 64 GiB chunk cache) runs over NBD
  (`nbds_max=1024`). It is driven by `EnvironmentRootfsStore.operation_lease` against the
  pre-warmed cache.
- **Signatures stubbed.** The spike was not given the production producer trust file,
  so signature checks are stubbed (`harness/s11lib.py:stub_trust`). Every content digest
  (roots, indexes, 256 KiB chunks) is still verified, and Ed25519 verification costs
  microseconds per component.
- **No prefetch on either path.** These prepared images carry no metadata hints
  (`metadata_hint_absent`), and cold runs have no startup traces. Nydus prefetch was also
  disabled, so both paths are demand-only.
- **Same kernel module on both paths.** Both used the rebuilt `erofs.ko`. That also
  shows the rebuilt module serves our current path unchanged.

**The fscache path.**
- One `nydusd singleton --fscache <dir> --fscache-threads 32`, with every image bound in
  one domain (`domain_id=s11`).
- Attach is what nydus-snapshotter does:
  1. download the bootstrap layer and extract `image/image.boot`;
  2. `PUT /api/v2/blobs` (bootstrap entry, registry backend, `cache.validate=true`);
  3. `mount -t erofs -o fsid=<id>,domain_id=s11 none <dir>`.

**Sandboxes.** Both paths compose the sandbox the same way as `OverlayRootfsManager` and
`direct_warden`:
- a host OverlayFS, with the image tree as the lower layer and a private upper and
  work directory;
- `runsc --platform=systrap --network=none` with the default `--overlay2=root:self`;
- the image's Env and WorkingDir, Docker's default capabilities, 2 vCPU and 2 GiB.

The sandbox runs `sleep`. Commands are timed as `runsc exec` calls:
- `python3 -c 'import sys'`;
- `git -C <WorkingDir> status`;
- `python3 -m pip --version`;
- `python3 -c 'import pytest'`.

**Cold and warm.**
- **Cold:** a new daemon, an empty cache, `drop_caches`, then attach, start and the four
  commands.
- **Warm:** the same commands again in the same sandbox.

**Runs.**
- **Sequential (`seq`):** each image is cold on its own. Run twice; the medians agreed
  within a few percent.
- **Parallel (`par`):** all 20 images start at once on a cold node. Run 4 times on each
  path, plus once with 3 sandboxes per image (60 sandboxes).
- **Host CPU:** busy jiffies from `/proc/stat`. Daemon CPU is the `utime+stime` of
  `nydusd` or of the backend process.
- **Cache disk:** the filesystem's used-bytes delta after `sync`. `cachefiles` keeps
  in-use backing files as unlinked tmpfiles, which `du` cannot see.

The harness is in [harness/](harness/): `s11lib.py`, `bench.py`, `verify_fscache.py`
and the step scripts.

## Module build recipe (exact kernel 7.0.0-30-generic)

### What the kernel requires

- **No built-in code needs the options.** Both are `bool` options of modules
  (`EROFS_FS=m`, `CACHEFILES=m`). `FSCACHE=y` is compiled into `netfs.ko`
  (`NETFS_SUPPORT=m`), which already exports everything both modules use.
- **Full-tree check.** Running `olddefconfig` on the full tree with the options enabled
  changes nothing outside the two options ([raw/modules/config.diff](raw/modules/config.diff)).
  The other differences in that diff come from the build machine: no `rustc` and a
  different compiler name. They are not caused by the options.
- **One dependent option.** `EROFS_FS_PAGE_CACHE_SHARE` becomes unavailable, because it
  `depends on ... !EROFS_FS_ONDEMAND`. Ubuntu does not set it today.
- **No other users.** No source outside `fs/erofs` and `fs/cachefiles` tests either
  option ([raw/modules/option-users-outside.txt](raw/modules/option-users-outside.txt)).
- **Conclusion:** a full kernel build is not needed.

### Signature policy and loading

- **Policy on these VMs:** lockdown `[none]`, `modules_disabled=0`, `sig_enforce=N`, no
  Secure Boot (`MODULE_SIG=y`, `MODULE_SIG_FORCE` unset).
- **Unsigned modules load.** They print `module verification failed: signature and/or
  required key missing - tainting kernel` and set taint 12288 (`O` out-of-tree plus `E`
  unsigned).
- **vermagic matches:** `7.0.0-30-generic SMP preempt mod_unload modversions`, the same as
  stock.
- **New dependency:** the rebuilt `erofs` now depends on `netfs`. `cachefiles` depended on
  it already.
- **Swap test:** `modprobe -r erofs cachefiles` (nothing was mounted), installing the new
  modules into `updates/`, `depmod` and `modprobe` all worked. `/dev/cachefiles`
  appeared, and every NBD-path run then used the rebuilt `erofs.ko`.

### Recipe

Each module builds in about 2.5 s.

```sh
KREL=7.0.0-30-generic; UBUNTU=7.0.0-30.30
apt-get install -y build-essential libelf-dev libdw-dev dwarves zstd \
  linux-source-7.0.0=$UBUNTU linux-headers-$KREL=$UBUNTU
tar -xjf /usr/src/linux-source-7.0.0/linux-source-7.0.0.tar.bz2   # Ubuntu-patched 7.0.12 source
S=$PWD/linux-source-7.0.0; H=/usr/src/linux-headers-$KREL
# Build in place (M= inside the extracted tree) against the running kernel's headers tree,
# so .config, vermagic and Module.symvers (modversions CRCs) are Ubuntu's, unchanged.
make -C $H M=$S/fs/cachefiles CC=x86_64-linux-gnu-gcc CONFIG_CACHEFILES=m CONFIG_CACHEFILES_ONDEMAND=y \
     KCFLAGS=-DCONFIG_CACHEFILES_ONDEMAND=1 -j"$(nproc)" modules
make -C $H M=$S/fs/erofs CC=x86_64-linux-gnu-gcc CONFIG_EROFS_FS=m CONFIG_EROFS_FS_ONDEMAND=y \
     KCFLAGS=-DCONFIG_EROFS_FS_ONDEMAND=1 -j"$(nproc)" modules
modinfo -F vermagic $S/fs/erofs/erofs.ko    # must equal: 7.0.0-30-generic SMP preempt mod_unload modversions
zstd -19 $S/fs/erofs/erofs.ko $S/fs/cachefiles/cachefiles.ko   # -> erofs.ko.zst, cachefiles.ko.zst
```

Notes on the recipe:
- **Build tools:** `libdw-dev` is required, because `gendwarfksyms` builds under
  `MODVERSIONS`.
- **Compiler name:** `CC=x86_64-linux-gnu-gcc` matches the name Ubuntu's build used
  (same GCC 15.2.0-16ubuntu1). Without it you get a harmless "compiler differs" warning.
- **Version naming:** the source tarball is `7.0.12` (`SUBLEVEL=12`). Building against
  the source tree's own `modules_prepare` therefore gives `7.0.12-30-generic`. Always
  build against the matching `linux-headers` tree.
- **Outputs:** sha256 sums of the spike's modules are in
  [raw/modules/sha256.txt](raw/modules/sha256.txt); build and load logs are in
  [raw/logs/](raw/logs/).

### Fitting this into the node bundle

These are notes for `scripts/repack_node_bundle.py` and `vm_init.py`.
- Add `erofs.ko.zst` and `cachefiles.ko.zst` to `runtime/kernel/7.0.0-30-generic/`, using
  `--kernel-release` and `--kernel-module-dir`.
- Add `"cachefiles"` and `"erofs"` to `RUNTIME_KERNEL_MODULES`.
- `vm_init` reuses depmod's index when a module's basename appears exactly once in
  `modules.dep`. `erofs.ko.zst` and `cachefiles.ko.zst` do, so the loader installs them
  over the stock files at `kernel/fs/{erofs,cachefiles}/` and needs no depmod.
- Stock `netfs.ko.zst` is present on the worker image and is the only dependency.
- The kernel-modules phase must run before anything loads stock `erofs`; today that is
  `environment_bootstrap`'s `modprobe erofs`. On a running node, swap modules only with
  no EROFS or cachefiles users.

## Results

### Sequential cold and warm starts

Values are medians per image across the 16 images that have `python3`. The other 4
(TMax 0, Terminal-Lego 1/3/5) have no Python and finish in about 20 ms on both paths.
Per-image values are in [raw/summary.json](raw/summary.json).

| Measure | Our EROFS/NBD | fscache + nydusd | fscache vs NBD |
| --- | ---: | ---: | ---: |
| Attach (resolve, fetch metadata, mount) | 0.165 s | 0.122 s | 0.74× |
| Ready (attach, overlay, `runsc create+start`) | 0.66 s | 0.60 s | 0.91× |
| Cold `python3 -c 'import sys'` | 0.51 s | 0.52 s | ≈1× |
| Cold `git status` (ScaleSWE repos) | 0.26 s | 0.23 s | 0.88× |
| Cold `python3 -m pip --version` | 3.45 s | 1.95 s | **0.57×** |
| Cold `import pytest` | 1.18 s | 0.88 s | 0.75× |
| Warm `import sys` / `git` / `pip` / `pytest` | 0.044 / 0.031 / 0.43 / 0.22 s | 0.049 / 0.039 / 0.47 / 0.24 s | 1.1× (fscache slightly slower warm) |
| Bytes fetched per image (cold, 4 commands) | 54.2 MB | 28.9 MB | 0.53× |
| Cache disk per image (fs-used delta) | 64.4 MB | 77.6 MB | 1.2× |
| Daemon CPU per image | 1.61 s | 1.00 s | 0.62× |
| Host busy CPU per image | 8.9 CPU-s | 5.2 CPU-s | 0.58× |

How to read the cache figures:
- **fscache caches more on disk** even though it fetches less. The cachefiles backing
  file holds the *uncompressed* blob address space, while our chunk cache holds the
  256 KiB chunks it fetched (`du -x` 54 MB plus metadata).
- **fs-used delta overstates both paths.** It also counts the sandbox upper and work
  directory, and on the fscache path the 0.2–3.2 MB bootstrap.
- **`nydusd` stores little of its own:** about 2.4 MB per image (`blob.meta`, `toc`,
  `digest`).

### Parallel cold start (20 distinct images on a cold node, 4 runs per path)

| Measure (range over 4 runs) | Our EROFS/NBD | fscache + nydusd |
| --- | ---: | ---: |
| Sandboxes OK | 19/20 every run¹ | 20, 20, 19, 20² |
| Wall time, all cold commands done | 8.24–8.39 s | 3.97–4.02 s |
| Attach median / max | 1.61–1.69 / 1.64–1.70 s | 0.24–0.27 / 0.42–0.44 s |
| First command done, median / max | 2.91–3.03 / 3.22–3.36 s | 1.24–1.29 / 1.31–1.42 s |
| Cold `pip --version`, median | 3.63–3.68 s | 1.73–1.79 s |
| Host busy CPU | 68.2–68.6 CPU-s (54% of 16 vCPU) | 37.5–38.5 CPU-s (63–64%) |
| Peak host CPU (0.5 s window) | 78–79% | 78–83% |
| Daemon CPU | 9.8–9.9 s | 6.7–7.0 s |
| Bytes fetched | 380 MB | 244–246 MB |
| Cache disk (fs-used delta, run 4) | 529 MB | 668 MB |

**With 3 sandboxes per image (60 sandboxes):**
- NBD: 12.6 s wall, 152 CPU-s, 380 MB, last first-command at 4.1 s.
- fscache: 5.6 s wall, 77 CPU-s, 230 MB, last first-command at 2.0 s.

Same-image sandboxes share one lower mount on both paths, so bytes fetched do not grow.
Both paths had 57 of 60 succeed, for the reasons in the footnotes.

¹ **NBD failures.** Terminal-Lego 2 and 4 are different prepared roots (different image
configs) with an identical component list. Our store keys a composition by component
list but compares the full environment, so whichever attaches second fails with
`environment config changed for an existing composition`. This is a production bug and
not specific to the spike; a follow-up task is filed.

Under a burst, our backend client also failed with `BlockingIOError(EAGAIN)`:
- **Cause:** the backend's Unix socket uses socketserver's default backlog of 5, and the
  client's timeout socket gets `EAGAIN` from `connect(2)`.
- **Scale:** 19 of 40 parallel calls failed in isolation
  ([raw/backend-client-eagain.json](raw/backend-client-eagain.json)).
- **Effect on these runs:** the earlier runs lost 3–8 of 20 sandboxes to it. The numbers
  above use a spike-only retry; a follow-up task is filed.

² **fscache failures.** `nydusd` answered `PUT /api/v2/blobs` with `500 Resource
temporarily unavailable (os error 11)` once in 80 binds. In the 60-sandbox run this
happened once, and the harness's retry then hit "bootstrap blob already exists" twice. A
production binder needs to retry the bind and to treat "already exists" as success.

### Correctness and behaviour (fscache path)

| Check | Result |
| --- | --- |
| File contents vs the OCI image | 900/900 sampled files match: 45 per image, including the 5 largest, compared by sha256 against the flattened OCI layers with whiteouts applied |
| gVisor | Processes run from the fscache tree through host OverlayFS and `runsc` with `overlay2=root:self`. Image files are visible, and writes to `/tmp` and `/root` land in the upper |
| Cross-image sharing in one domain | 64 blob references → 41 distinct blob objects (layer-identical blobs: 4 `responses`, 7 TMax/T-Lego sharing a base, and so on) |
| Chunk-dict children (`--chunk-dict` against a sibling) | A ScaleSWE sibling adds **2.66–2.75 MB** of new blob data, against 434–456 MB stand-alone. After its base was read, reading the child's whole tree fetched **0 bytes** beyond its 2.8–2.9 MB bootstrap |
| Chunk digest validation (`cache.validate=true`) | One uncompressed image's blob was corrupted (397 flipped bytes). With validation: 76 of 2,628 files returned `EIO`, 0 were wrong, and `nydusd` logged `data digest value doesn't match`. Without validation: the same 76 files were **silently wrong** |
| Same image mounted twice in one domain | The kernel refuses (`<fsid> already exists in domain`), so sandboxes must share one mount per image |
| `kill -9 nydusd` mid-read, no supervisor | Reads on the mount fail at once with `ENOBUFS` (errno 105), and keep failing after a new `nydusd` starts. Only `umount` plus a new mount recovers (the full tree then reads correctly). Every mount on the node is lost |
| `kill -9 nydusd` mid-read, with a supervisor holding `/dev/cachefiles` | Reads **stall** with no errors. A new `nydusd --upgrade` takes over (`/api/v1/daemon/fuse/takeover`, then `start`, in 0.06 s) and reads resume: 33,647 files read, 0 errors, 0 wrong. This is the kernel's ondemand "restore" failover; nydus-snapshotter normally plays the supervisor |
| `nydusd` API robustness | `GET /api/v2/blobs?domain_id=…` hits a `todo!()` and panics the API thread |

### Conversion and storage (20 images)

| Measure | Value |
| --- | ---: |
| `nydusify convert` (fs-version 6, zstd, 1 MiB chunks, 4 in parallel) | 2–29 s per image, 20/20 OK |
| OCI layers (compressed) | 5.51 GB |
| Our EROFS components, summed / distinct | 8.82 / 5.08 GB |
| Nydus data blobs, summed / distinct | 5.44 / 2.89 GB |
| Nydus bootstraps | 35.6 MB |

Per-image numbers are in [raw/storage.json](raw/storage.json). Dedupe beyond layer
identity is S10's question.

## Integration notes for C2.13

### Signing: what we sign and what nydusd checks

**What we sign.** The signed root should cover:
- the image config;
- the bootstrap digest (the sha256 of `image.boot`, or of the bootstrap layer);
- the ordered data-blob digests. These are the blob IDs, the sha256 of the compressed
  blobs.

**What nydusd checks, and what it does not:**
- **Bootstrap:** nydusd does not verify it at all. It trusts the `metadata_path` file it
  is given, so the worker must check the bootstrap against the root before binding.
- **Chunks:** the bootstrap carries a BLAKE3 digest per chunk (`HASH_BLAKE3 |
  INLINED_CHUNK_DIGEST`). With `cache.validate=true`, nydusd checks each chunk when it
  fills the cache.
- **Cached data:** once a chunk is in the cachefiles backing file, the kernel reads it
  directly and nothing re-verifies it. Tampering or corruption of the cache disk is not
  detected after the fill.
- **Our path today** verifies 256 KiB chunk digests in our own process before install.

### Prefetch

**Nydus.** A Nydus prefetch list is fixed at conversion time (`--prefetch-patterns`,
stored in the bootstrap). `nydusd` replays it after bind when `cache.prefetch.enable` is
set.

**Our hints and traces** cannot be fed in at runtime this way:
- metadata hints are signed per component;
- startup traces are learned on the node.

**Options:**
- bake a trace-derived list into the bootstrap at conversion, which changes the signed
  bootstrap per update;
- or have our own warmer read the traced files through the mount, so the fill happens
  in-kernel.

The NBD path keeps today's chunk-index hints and traces unchanged.

### Cache sizing and eviction

- **Who manages the cache.** In ondemand mode `nydusd` owns the cache directory, and
  `cachefilesd` culling does not apply.
- **No budget or LRU.** There is no byte budget and no LRU. The only eviction is
  per-blob `DELETE /api/v2/blobs?blob_id=` (`cull_cache`), and only for blobs no mount
  uses.
- **Size per blob.** A backing file is sparse but sized to the *uncompressed* blob, so
  disk use is the uncompressed bytes touched. Here that was about 1.2× our chunk cache
  for the same commands, and 1.26× in the parallel run.
- **What we would need.** Our own GC over blob reference counts, plus a disk watermark.
  Partial eviction of a hot blob is impossible.
- **What our path has.** Our chunk cache has a byte budget (`cache_bytes`, 128 GiB in
  production).

### nydusd as a per-worker systemd service

- **Daemon count:** one `nydusd singleton` per worker binds `/dev/cachefiles`. One bind
  per cache tag; a second daemon would need its own `--fscache-tag`.
- **Restarts must not be plain restarts.** Every restart has to be a failover:
  - a small supervisor holds the `/dev/cachefiles` fd and nydusd's saved state, through
    `--supervisor` and `--id`, after `PUT /api/v1/daemon/fuse/sendfd` on each bind;
  - the replacement runs with `--upgrade`, followed by `fuse/takeover` and `start`.

  The supervisor could be our node agent, or systemd's fd store with a shim.
- **Plain `Restart=always` destroys every mount on the node.** The service must be
  ordered after the module phase and before the node agent.
- **Socket and API.** The API socket should be root-only. The binder must:
  - retry `EAGAIN`;
  - treat "already exists" as bound;
  - avoid the panicking v2 GET.

### Mounts and daemons for 500+ sandboxes per node

- **Per node:** one `nydusd`.
- **Per image:** one EROFS mount per distinct image, which the kernel enforces per
  domain. Sandboxes of the same image share it through their OverlayFS lower.
- **Kernel objects:** one fscache object per distinct blob in the domain, plus one per
  bootstrap, each holding an fd in `nydusd`.
- **Worst case, 500 distinct images:** 500 mounts and roughly 500 bootstrap objects.
  The blobs number in the low thousands with layer-wise conversion, and fewer with a
  corpus chunk dictionary, where most images point at shared dictionary blobs. That is
  far below `nydusd`'s raised `RLIMIT_NOFILE` of 1,000,000.
- **No device pool.** Our path needs one `/dev/nbdN` per distinct mounted component,
  bounded by `nbds_max=1024`.
- **Not measured beyond 60 concurrent sandboxes.**

### Failure modes

1. **`nydusd` death without a supervisor.** All mounts on the node return `ENOBUFS`
   until they are unmounted and remounted. That kills every running sandbox's image.
2. **`nydusd` death with a supervisor.** I/O stalls until takeover. A stuck or slow
   takeover hangs every sandbox, so it needs a watchdog.
3. **Registry or backend errors.** These surface to the guest as `EIO`, as with our path.
   Retries use `nydusd`'s backend `retry_limit`.
4. **Bind under burst:** occasional `EAGAIN`. Duplicate binds are rejected.
5. **Cache-disk corruption after fill** goes undetected, as described under Signing.
6. **Deprecated upstream.** A kernel bump may delete `fs/erofs/fscache.c` outright. We
   would then have to forward-port removed code, or switch paths under pressure.
7. **Secure Boot or lockdown** on any future host or image makes the unsigned modules
   unloadable. Signing them needs our own MOK enrolment.

### Kernel-bump maintenance cost

**Per bump:**
1. Install `linux-source-7.0.0=<abi>` and `linux-headers-<krel>`.
2. Build two modules, about 5 s.
3. zstd them, run `repack_node_bundle --kernel-release`, then run a mount and failover
   smoke test.

The build is cheap to script and to run in CI.

**What it really costs** is ownership:
- **We would be the only tester.** Canonical does not enable or test the option, so
  every Ubuntu stable update is untested for us.
- **The feature has an announced end of life.** Ubuntu's 7.0 kernel already marks it
  deprecated.
- **Module ABI drift.** The rebuilt `erofs` changes the module's ABI surface (it now
  depends on `netfs`). Ubuntu's `netfs` and `fscache` changes in a stable update could
  break it without notice.

## Recommendation

**1. Do not adopt fscache for C2.13.**
- Keep the plan's EROFS multi-device mount of Nydus RAFS v6 over our NBD backend.
- Keep the `nydusd` fallback (FUSE or userfaultfd block mode) as written.
- The fscache path works and is fast, but it bets the worker image path on a deprecated
  kernel feature in unsigned out-of-tree modules, and it needs a supervisor to survive a
  daemon restart.

**2. Close the measured gap inside the NBD path.** The inputs to S10 and C2.13:
- serve zstd-compressed Nydus chunks and decompress in the backend, which the plan
  already intends; this alone halves bytes fetched;
- fetch 1 MiB units with coalescing;
- let attaches proceed concurrently rather than one at a time; NBD attach median was
  1.6 s under a 20-way burst;
- move the hot read path out of Python, or batch it; the backend spent 9.8 s of CPU per
  20-sandbox burst against `nydusd`'s 6.8 s, and host CPU was 68 against 38 CPU-s.

Target: within about 1.3× of the fscache numbers above (20-way cold burst at or under
5.5 s, cold `pip --version` at or under 2.5 s).

**3. Conditions to revisit fscache.** All of these must hold:
- upstream keeps on-demand mode, or names a successor we can adopt. The successor to
  evaluate is file-backed EROFS (`EROFS_FS_BACKED_BY_FILE=y`, enabled today) over a
  sparse local file filled through fanotify pre-content events (`FANOTIFY=y`,
  `FANOTIFY_ACCESS_PERMISSIONS=y` here);
- the NBD path misses the target in item 2;
- we commit to signed module builds per kernel bump;
- we commit to a supervised `nydusd` with takeover, and to our own blob-level cache GC.

**4. Fix the two production bugs this spike found, whatever the path.** Follow-up tasks
are filed for both:
- the environment backend's socket backlog, which causes `EAGAIN` under burst;
- the composition collision between same-filesystem images with different configs.

## Risks and limits of this spike

- **Not a production worker.** One ccx43, not a production CCX63, and dedicated rather
  than the requested shared vCPU. The registries were local loopback, so WAN and
  production-registry latency are not modelled. Both paths had that advantage equally.
- **No prefetch.** Both paths ran demand-only. Our metadata hints are absent from these
  images, and our trace prefetch was not exercised. With traces, our second-run cold
  starts would improve; the fscache path would need the warmer described above.
- **Different chunking.** The fscache path's lead partly reflects Nydus's compressed
  1 MiB chunks rather than fscache itself. S10's NBD multi-device run with the same blobs
  is the comparison that isolates the transport.
- **NBD runs had one fewer sandbox.** NBD parallel runs completed 19 of 20 sandboxes
  (Terminal-Lego 4 failed early on the collision), which slightly flatters NBD's CPU and
  bytes.
- **Signatures stubbed** on our path, as described in Method.
- **Narrow failover coverage.** Failover was tested once per mode on one image, with
  `kill -9` during a full-tree read. Takeover under hundreds of in-flight requests, and
  repeated crashes, were not tested.
- **Invalid early cache figures.** In the first two rounds, `du` crossed into mounted
  component trees on the NBD path and could not see `cachefiles` tmpfiles. Only runs from
  run 3 on ([raw/logs/run3.log](raw/logs/run3.log)) report cache disk. Their `seq` files
  and `par-r4`/`x3` files carry the corrected `cache_usage_bytes`; timing and byte
  figures in all runs are valid.

## Files

- **Summaries:** [raw/summary.json](raw/summary.json) holds the summaries and per-image
  values.
- **Benchmark runs:**
  - sequential: `raw/bench-{fscache,nbd}-seq.json`;
  - parallel: `raw/bench-*-par-r{1..4}.json` and `raw/bench-*-par-x3.json`.
- **fscache checks:** [raw/verify-fscache.json](raw/verify-fscache.json) covers
  contents, sharing, chunk dictionary, validation and failover.
- **Storage and conversion:**
  - storage: `raw/storage.json`, `raw/environments.json`;
  - conversion: `raw/convert/`.
- **Modules:**
  - build, config and load evidence: `raw/modules/`;
  - logs: `raw/logs/02b-modules.log`, `raw/logs/03-load.log`;
  - kernel messages: `raw/dmesg-summary.txt`.
- **Harness:** `harness/` holds the step scripts `01`–`07`, `s11lib.py`, `bench.py`,
  `verify_fscache.py`, `nbd_backend.py` and the run scripts.
