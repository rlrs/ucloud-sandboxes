# S13: file-backed EROFS + fanotify pre-content hooks instead of NBD (C2.13), 2026-10-02

The question: can EROFS file-backed mounts plus fanotify pre-content (HSM) events replace our NBD
block backend for serving chunk-store images on workers? Each image gets a sparse backing file that
our daemon fills on demand from the chunk store; once a range is filled, reads stay in the kernel.
Design context: [`chunk-store-design.md`](../../chunk-store-design.md) (§4, Decisions, S12 result).
Earlier spikes: [S10](../nydus-spike-2026-10-02/README.md) (RAFS v6 over NBD),
[S11](../fscache-spike-2026-10-02/README.md) (fscache on-demand, whose Kconfig names fanotify
pre-content hooks as its successor) and [S12](../s3-chunk-spike-2026-10-02/README.md) (S3, packs,
the NBD backend over packs).

## Verdict

**Replace NBD later, in C2.1's native daemon, not in M1.** No gate failed hard. The mechanism works
and is fast in the right layout. But it fails open where NBD fails closed, and it only pays off with
a per-blob layout and a native daemon that M1 does not have.

**What works:**
- **Pre-content events reach the daemon from EROFS's own reads**, both `read()` and mmap faults,
  with the byte range (gate 1).
- **All 7 of S10's images read back identically to NBD**, entry for entry, over the full tree
  (gate 3).
- **Stock kernel only:** `CONFIG_FANOTIFY_ACCESS_PERMISSIONS=y` and
  `CONFIG_EROFS_FS_BACKED_BY_FILE=y`, with no out-of-tree module, no NBD device pool, no
  `nbds_max` and no NBD timeouts. This is the successor that S11's deprecated fscache path names.
- **Speed matches NBD in the multi-device layout**, measured with Python daemons against S12's
  Python NBD backend over the same local packs:
  - **Sequential cold commands:** `import sys` 0.45 s against 0.47 s, `git status` 0.31 against
    0.31 s, `pip --version` 1.50 against 1.81 s.
  - **20-way burst:** 5.6–6.2 s against 7.1–7.2 s, at the same host CPU (43–45 against 49–50
    CPU-s), with `pip` at 2.0 against 3.2–3.5 s.
  - It reads 15–20% fewer pack bytes.
- **Once a backing file is complete and its mark removed, reads never leave the kernel** (0 events
  on a full re-read).
- **Disk sharing works** (gate 4). Reflink from a shared chunk cache costs a median 29 MB per extra
  image, against 299 MB for plain copies.

**Why not in M1:**
1. **It fails open** (gate 2). If the fanotify group is released, by the daemon dying or its fd
   closing, the kernel allows every pending event, and EROFS reads unfilled holes as zeros: silently,
   and still cached after a restart. Today's NBD backend, killed the same way, gives EIO within
   45 ms.
   - Safe operation needs an fd store holding the group and the pre-mark write fds, a journal of
     in-flight events answered by fd number on takeover (both demonstrated), and a rule that losing
     the group retires every file-backed mount on the node.
   - The same hazard bit S13 itself: a shared blob file that lacked one image's chunk entries
     served zeros to 3–5 of 100 sandboxes until fixed.
2. **The unified one-file layout (M1's device model) is slow.** A kernel quirk in
   `erofs_fileio_scan_folio` (file-backed mode only; the block path maps through iomap) turns every
   4 KiB page of a flat multi-blob image into its own read and its own event. That makes 102k events for a 20-way burst and 502k for 100-way. Under that
   load the Python daemon falls behind (20.0 s against NBD's 7.2 s at 20-way, 86 s against 28 s at
   100-way), and gVisor's systrap spins while it waits (2,870 host CPU-s against 203). The
   per-blob layout (`-o device=`) avoids the quirk, but it changes M1's attach model.
3. **At 100-way even the per-blob layout loses** in Python (37 s against 27.6 s): attach parsing
   and per-event overhead saturate one interpreter. A native daemon is C2.1's job anyway.

**For C2.1**, the fanotify path should replace NBD if the native daemon:
- uses per-layer blob files carrying each layer's complete chunk table, plus a complete bootstrap
  file per image, mounted with `-o device=` and `-o directio`, with fills written with `O_DIRECT`
  or by reflink;
- denies (EIO) any range it cannot place;
- keeps its group fd and write fds in systemd's fd store, journals events and takes over on
  restart, and retires the node's mounts if the group is lost;
- removes a blob's mark once the blob is complete;
- sustains a 100-way burst at or under NBD's 27.6 s.

Reconsider the unified layout if upstream fixes the flat-device merge check.

**Page cache (gate 5, S12's item e).** Memory per extra distinct image (the slope of `Cached` over
25 images on 4 bases, file-backed, `-o directio`):

| Granularity | Per extra image |
| --- | ---: |
| per-image merged mounts | 21.0 MB |
| per-layer mounts stacked with OverlayFS | 12.3 MB |
| `inode_share` (one domain, fingerprinted mkfs.erofs images) | **3.1 MB** |

- **Buffered file-backed mounts cost about twice as much** (40, 21 and 5.7 MB), because pages are
  cached in EROFS and again in the backing file. So `-o directio` is mandatory for file-backed
  workers.
- **`inode_share` works on file-backed mounts.** The kernel expects the value of the xattr named by
  the superblock's `ishare_xattr_prefix_id` long prefix. mkfs.erofs 1.9 writes it with
  `--xattr-inode-digest=<name>` as `sha256:` plus the 32-byte content digest.
- **RAFS from nydus-image cannot carry fingerprints.** Sharing therefore means building images with
  mkfs.erofs 1.9 (`--tar`, `--chunksize=262144 --blobdev`, `--xattr-inode-digest`), or teaching our
  converter to emit the xattrs. It also needs the experimental, out-of-tree `erofs.ko`.
- **Recommended granularity:**
  - **M1:** keep per-image merged mounts. Per-layer stacking saves 9 MB per image at the price of
    OverlayFS stacks and RAFS whiteout handling.
  - **Later:** move to `inode_share` with mkfs.erofs-built images once the module is acceptable.

**Side finding, which blocks M1's full-tree gate.** nydus-image v2.4.5 `--repeatable`, which the
design and `chunk_convert.py` use, writes every file owner as 0:0. All 7 images lost their non-root
owners (for example `/etc/shadow`'s group 42, and `/home/nonroot` 1000:1000). Without the flag,
owners are kept.

## What ran

- **VM:** one CCX63 (`sandboxes-spike-s13`, snapshot 438728121, 10.42.0.47, plus a public IPv4 for
  GitHub and the Ubuntu archive). VM init never ran.
  - **Runtime:** created at 19:00:35Z and deleted at 20:39:42Z, so **1 h 39 min (1.65 VM-hours)**.
  - **Cleanup:** `GET /servers/168425130` then returned 404, and no server named
    `sandboxes-spike-s13` remained. The gateway's `/root/s13-staging/` and its `known_hosts` entry
    for 10.42.0.47 were removed. S12's leftovers on the gateway were already gone when S13 started.
- **Kernel:** stock `7.0.0-30-generic`.
  - **Stock `erofs.ko`:** gates 1 and 2, gate 3's tree check, sequential runs and first bursts, and
    the NBD death control.
  - **Rebuilt `erofs.ko`:** from then on, an out-of-tree build with
    `CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y` (the S11/S12 recipe,
    [`scripts/build_erofs_ishare.sh`](scripts/build_erofs_ishare.sh)). That covers gate 4, the
    shared-blob burst reruns (`-fix`), gate 5 and the owner-aware tree check. It differs from stock
    only by that option, which acts only with `-o inode_share`.
- **Tools:**
  - Nydus v2.4.5 static release from GitHub (sha256 `1ad7b793…19072`, as S10/S12);
  - erofs-utils 1.9, xfsprogs 6.18, Python 3.14.4;
  - runsc from the snapshot (`/usr/local/libexec/ucloud-gvisor/runsc`);
  - no Docker Hub pulls.
- **Production access was read-only:** manifests and blobs from `10.42.0.2:5000`. Nothing was
  written to the registry, the gateway's config, `images.sqlite`, services or S3. S3 was not read.
- **Corpus:** S10's 181-image sample fetched again (58.6 GB unique layers in 187 s), converted per
  layer with S10's `convert.py` (256 KiB chunks, sha256, zstd, `--repeatable`, no dictionary) and
  packed with S12's `packer.py` into a local pack store on the VM's disk, which stands in for the
  store node: **623 packs, 16.86 GB, 874,989 unique chunks**, identical to S12. S12's 64-image
  page-cache set was re-selected with S12's `select_pc.py` (same seed; the same 64 images, 147
  layers, 6.28 GB) and packed into the same index: 714 packs, 17.49 GB
  ([`raw/packer-main.json`](raw/packer-main.json), [`raw/packer-pc.json`](raw/packer-pc.json),
  [`raw/pc-selection.json`](raw/pc-selection.json)).
- **The fill daemon** ([`scripts/fand.py`](scripts/fand.py), Python, like S12's NBD backend): one
  `FAN_CLASS_PRE_CONTENT` group per node; per backing file an inode mark (`FAN_PRE_ACCESS`) placed
  before the mount; per event it maps the range to chunk-map entries, fetches the missing 256 KiB
  chunks from the packs (misses on one pack merged into ≤ 1 MiB windows, gaps < 64 KiB, as S12's
  backend), checks `len == ulen` and `sha256 == id` after zstd, writes them at their offsets and
  answers `FAN_ALLOW`; any failure answers `FAN_DENY` with EIO. A 2 GiB in-memory chunk LRU is
  shared by all images, like S12's backend's memory tier. Layouts:
  - `image`: one sparse file per image holding its unified address space (bootstrap at 0, blob *i*
    at `mapped_blkaddr × 4096`), the space S12's NBD device exposes;
  - `multidev`: the image's bootstrap as a complete small file, plus one sparse file per blob
    (layer), shared by every image that has that blob, mounted with `-o device=…` per blob.
- **Today's path for comparison:** S12's `s3nbd.py --fetch local` over the same packs (memory and
  disk chunk cache, `concurrent_misses` 32), one `/dev/nbdN` per image, `mount -t erofs`.
- **Incidents, all rerun:**
  - **Takeover deadlock.** The first takeover test hung: its successor wrote through a file
    descriptor it had opened itself (gate 2). It was rerun with handed-over fds.
  - **Shared-blob bug.** The first multi-device bursts served zeros (gate 3). They were rerun after
    the fix as the `-fix` files.
  - **Attach bug.** A follow-up `fand.py` change broke every attach (a property without a setter),
    and the first pass of gates 4 and 5 failed at once. Both gates were rerun complete by
    [`scripts/run_rest3.sh`](scripts/run_rest3.sh).

## Gate 1: pre-content events from EROFS's own reads

**Pass.** EROFS's data reads of its backing file, issued from inside the kernel, raise
`FAN_PRE_ACCESS` on that file, both for `read()` and for page faults on files mmapped inside the
mount ([`raw/gate1.json`](raw/gate1.json), [`scripts/gate1.py`](scripts/gate1.py),
[`scripts/fanl.py`](scripts/fanl.py)).

- **Image:** S10's Terminal-Lego 004302 (sample index 130): 9,844 files and 266 MB of file data.
- **RAFS layout:** one 292 MB sparse file, with the 2.3 MB bootstrap at offset 0 and the 5 blobs,
  uncompressed, at `mapped_blkaddr × 4096` ([`scripts/flatten.py`](scripts/flatten.py)).
- **The listener** copies each requested range from a complete copy of the same file, then answers
  `FAN_ALLOW`.

| Case | Mount | Content vs the complete image | Events | Event range p50 / p90 / max |
| --- | --- | --- | ---: | --- |
| RAFS unified file, `read()` of every file | ok | 9,844 / 9,844 equal | 45,219 | 4 / 4 / 128 KiB |
| RAFS, mmap page faults only (12 largest files, 62.5 MB) | ok | 12 / 12 equal | 5,032 | 4 / 16 / 128 KiB |
| RAFS, `-o directio` | ok | 9,844 equal | 45,219 | 4 / 4 / 128 KiB |
| RAFS, no listener | ok | **9,676 differ** (holes read as zeros) | – | – |
| RAFS, mark placed only after the mount | ok | **9,676 differ** | **0** | – |
| mkfs.erofs 1.9 plain image, wholly sparse | **fails**: "cannot find valid erofs superblock" | – | 0 | – |
| Plain image, superblock block prefilled | ok, but every inode reads as zeros ("bogus i_mode (0)") | 0 / 9,844 | 0 | – |
| Plain image, complete, listener attached | ok | 9,844 equal | 6,337 | 20 / 128 / 128 KiB |
| mkfs.erofs `--chunksize=262144 --blobdev`: complete metadata file plus a sparse blob, `-o device=` | ok | 9,844 equal | 11,268 | 8 / 64 / 128 KiB |
| The same, mmap faults only | ok | 12 / 12 equal | 662 | 128 / 128 / 128 KiB |
| mmap of the marked backing file itself, outside EROFS | – | – | 1, at `mmap()`, for the whole 64 MiB mapping; none at the fault | – |

**Event fields.**
- **Header:** a 24-byte `fanotify_event_metadata`: `vers` 3, `mask` 0x100000 (`FAN_PRE_ACCESS`),
  `fd` (the backing file, opened for the listener) and `pid` (the reading task; in gate 1 always
  the reader process, not a kernel worker).
- **Range:** one 24-byte `FAN_EVENT_INFO_TYPE_RANGE` (6) record follows, with `offset` and `count`
  in bytes of the backing file. It is always present on pre-content events; no init flag is needed.
  Offsets were always 4 KiB-aligned.

**What the kernel does** (7.0.0-30; the passages are in [`raw/kernel-source-notes.txt`](raw/kernel-source-notes.txt)):
- **Data reads raise events.** File-backed data I/O goes `erofs_fileio_rq_submit` →
  `vfs_iocb_iter_read` → `rw_verify_area` → `fsnotify_file_area_perm` → `fsnotify_pre_content`,
  buffered or with `-o directio`. Page faults inside the mount take the same path through
  `read_folio` and readahead.
- **Metadata reads do not.** Superblock, inode and xattr reads use `erofs_bread` on the backing
  file's `f_mapping` (`erofs_init_metabuf` → `read_mapping_folio`), which never calls
  `rw_verify_area`. So EROFS metadata must be on disk before the mount.
  - Directory blocks are read through the directory inode's own mapping, and do raise events
    (gate 2's stacks show one).
  - RAFS keeps all metadata in the bootstrap, which the daemon writes at attach, so RAFS works.
  - A plain single-file mkfs.erofs image interleaves metadata and data, and inlines small files into
    metadata, so it cannot be filled on demand. With `--blobdev` it can.
- **The mark must exist before EROFS opens the file.** `fsnotify_open_perm_and_set_mode` decides at
  `open()` whether a file raises pre-content events. A file opened while its inode had no
  pre-content mark never raises them, whatever is marked later.
- **Only ext4, XFS and btrfs opt in** (`SB_I_ALLOW_HSM`). On other filesystems, tmpfs for example,
  `fanotify_mark` returns `EOPNOTSUPP`.
- **mmap of the backing file** raises one event at `mmap()` time for the mapped range
  (`fsnotify_mmap_perm` in `mm/util.c`); faults raise none. EROFS never mmaps its backing file, so
  this does not matter here.
- **Writes raise events too.** The daemon must write through a descriptor opened before the mark.
  A descriptor opened while the mark exists blocks its own `pwrite` on its own group (gate 2 hit
  this as a deadlock).

**Why RAFS unified files raise one event per 4 KiB.** `erofs_fileio_scan_folio` extends a request
only if `map->m_pa + ofs == io->dev.m_pa`.
- In flat mode (one file, blobs placed by `mapped_blkaddr`), `erofs_map_dev` adds the device's
  `uniaddr` to `io->dev.m_pa` but not to `map->m_pa`. So for every chunk on an extra device the
  check fails at every folio, and each 4 KiB page becomes its own backing read and its own event.
- Device 0 (plain mkfs.erofs) and non-flat multi-device mounts (`-o device=`) merge requests up to
  128 KiB.
- This looks like an upstream bug. S13 avoids it with per-blob files (`multidev`, gates 3–5).

**Kernel facts** ([`raw/kernel-facts.txt`](raw/kernel-facts.txt)):
- **Build:** `7.0.0-30-generic #30-Ubuntu SMP PREEMPT_DYNAMIC Fri Jul 31 2026` (source 7.0.12).
- **Config:** `CONFIG_FANOTIFY=y`, `CONFIG_FANOTIFY_ACCESS_PERMISSIONS=y`, `CONFIG_EROFS_FS=m`,
  `CONFIG_EROFS_FS_BACKED_BY_FILE=y`, `CONFIG_FS_STACK=y`; `CONFIG_EROFS_FS_ONDEMAND` and
  `CONFIG_EROFS_FS_PAGE_CACHE_SHARE` unset.
- **uapi:** `FAN_PRE_ACCESS`, `FAN_EVENT_INFO_TYPE_RANGE` and `FAN_DENY_ERRNO` are in the header.
- **Limits:** `fs.fanotify.max_user_groups` 128, `max_user_marks` 1,048,576 and
  `max_queued_events` 16,384 (the daemon uses `FAN_UNLIMITED_QUEUE`). There is a new
  `fs.fanotify.watchdog_timeout`, 0 (off).

## Gate 2: listener death

**The kernel fails open.** A pre-content group can be released by the listener being killed, by it
exiting, or by it closing the fd. In every case `fanotify_release` answers each pending and queued
permission event with `FAN_ALLOW` and removes the group's marks. EROFS then reads the unfilled holes
and gets zeros, with no error and no log line ([`raw/gate2.json`](raw/gate2.json),
[`scripts/gate2.py`](scripts/gate2.py), [`scripts/fdholder.py`](scripts/fdholder.py)).

| Case (a 24 MB `.so`, unfilled) | Result |
| --- | --- |
| Listener SIGKILLed while it holds the event | the blocked read returns at once: **24,325,657 bytes, every page zero** |
| Listener exits normally (group fd closed) | the same |
| A new read after the listener is gone | **zeros** (a 10.6 MB file, every page) |
| A new listener (new group, new mark on the same inode) on the same mount | an untouched file reads correctly. The file read during the gap **still reads zeros**, from the EROFS page cache; after `drop_caches` it reads correctly |
| Deny with `FAN_DENY_ERRNO(e)` for e in EIO, EPERM, EAGAIN, ENOSPC, EBUSY, ETXTBSY, EDQUOT | accepted; the reader always gets **EIO** (EROFS turns any backing error into EIO) |
| Deny with EACCES or ENOENT | the response `write()` fails with EINVAL and the event stays pending, so the reader blocks (D state, killable) |
| Event never answered, group alive | the reader waits in `fanotify_get_response` (`TASK_KILLABLE`); `kill -9` ends it at once. The watchdog sysctl only logs |
| **Takeover.** A holder process keeps the group fd and the daemon's pre-mark write fds, as systemd's fd store would. Daemon A is SIGKILLed while holding an event; B takes the fds | the pending read waits (D state) until B, after filling, answers A's journaled event **by its fd number**. The read then completes **correctly**; answering EIO gives EIO. Events queued while no daemon ran are read and served by B |
| Takeover without answering A's event | that read stays blocked until killed; later reads are served |
| Takeover where B opens the backing file itself | **deadlock**: B's `pwrite` raises a pre-content event on its own group and waits for itself ([`raw/gate2-takeover-deadlock.txt`](raw/gate2-takeover-deadlock.txt)) |
| The holder dies too (group released) | the pending read returns **zeros** |
| **Control: today's NBD backend SIGKILLed during a read** ([`raw/nbd-death.json`](raw/nbd-death.json)) | the read fails with **EIO** within 45 ms ("shutting down sockets", "I/O error, dev nbd900"), and later reads fail with EIO too. NBD fails closed |

**What the kernel guarantees** (`fs/notify/fanotify/fanotify_user.c`, `fanotify.c`):
- **Release allows.** `fanotify_release` says "Process all permission events on access_list and
  notification queue and simulate reply from userspace", and calls
  `finish_permission_event(..., FAN_ALLOW, ...)`. No option makes it deny instead.
- **Responses match by fd number.** `process_access_response` matches a response to a pending event
  by its fd number only. Any process holding the group fd can answer, so a successor can resolve its
  predecessor's events if it knows their numbers.
- **Custom errnos are limited.** Only pre-content groups may set one, and only from {EIO, EPERM,
  EBUSY, ETXTBSY, EAGAIN, ENOSPC, EDQUOT}. Anything else returns `-EINVAL` and leaves the event
  pending.
- **Waits are killable, and unbounded.** `fanotify_get_response` waits
  `TASK_KILLABLE | TASK_FREEZABLE`. An unanswered event blocks its reader until SIGKILL.
- **Re-registration works without remounting.** The HSM mode belongs to EROFS's open file, set at
  mount, and each read checks it against the inode's current marks.

**What production would need.** A daemon restart must be a takeover, never a fresh group:
- the group fd and each backing file's write fd live in an fd store that outlives the daemon
  (systemd `FileDescriptorStoreMax=` with `FileDescriptorStorePreserve=yes`, or a minimal holder);
- the daemon journals each event (fd, inode, range) before working on it. Its successor fills and
  answers the journaled events by number, then reads the queue;
- if the group is ever released (holder and daemon both gone), every file-backed mount on the node
  may hold zeros in its page cache and in reads already returned. The node must unmount them all and
  end their sandboxes, as after S11's unsupervised `nydusd` loss, but here no error shows it
  happened.

## Gate 3: correctness and speed under gVisor

**Correct, and as fast as NBD in the multi-device layout, but the Python daemon does not keep up
with the unified layout's 4 KiB events under a burst** ([`scripts/coldfan.py`](scripts/coldfan.py),
[`raw/summary.json`](raw/summary.json)).

### Correctness: full trees of S10's 7 images ([`raw/tree.json`](raw/tree.json), [`raw/tree-owners.json`](raw/tree-owners.json))

Each image was mounted cold through `fan:demand` and through S12's NBD backend, and every entry was
read. The comparison covers names, types, modes, owners, sizes, the sha256 of every file, symlink
targets, file mtimes, xattrs (overlay ones excluded) and hardlink groups. The reference is the OCI
layers with whiteouts applied (M1's `chunk_convert.expected_tree` semantics, streamed).

- **fanotify equals NBD on all 7 images, entry for entry** (54,613 entries for ScaleSWE, up to
  97,058 for SWE-Lego).
- **Against the OCI image:** the **only** differences are owners. Every entry
  that has a non-root uid or gid in the OCI layers reads back as 0:0: 12–25 entries per image, and
  5,390 for R2E-Gym, whose `/testbed` tree is user-owned. The cause is the converter, not the
  transport (side finding below). Kind, mode, size, sha256, symlink target, mtime and xattrs match
  for every entry, with nothing missing or extra. Hardlink groups also match OCI on the 4 images
  where the comparison reached them (0, 90, 130, 170); on the other 3 it stopped at 20 owner
  differences, and they match NBD.
- **Every chunk-map entry was filled** by the full read (for example 42,674 of 42,674 for image 0).
  The daemon read the same compressed bytes from the packs as NBD did (668 MB for image 0).
- **Full-tree read time**, fanotify against NBD, ranged from 0.93× to 1.22× (for example 41.6
  against 34.4 s for image 0, and 8.0 against 8.6 s for image 130).
- **Reads after filling.** After dropping caches, a second full read still raised one event per
  4 KiB page miss: 45k–197k events at 50–65 µs each. This is the cost of a mark that stays in place.
  With `--unmark-when-full`, the daemon removes the mark once every entry is filled, and a re-read
  raised **0 events** (3.7 s against 8.9 s with the mark kept, image 130 with `-o directio`,
  [`raw/mdsmoke.json`](raw/mdsmoke.json)).
  Only then do reads stay in the kernel.

### Sequential cold commands under runsc

The 7 images, 2 repetitions, each cycle with a fresh backend, empty caches and `drop_caches`. The
command runs under `runsc --platform=systrap --network=none run`, with the rootfs served by the
gofer from a host OverlayFS over the EROFS mount (S12's `coldrun.py`). The table gives medians over
14 cycles, as `import sys` / `git status` / `pip --version`.

| Path | Cold, s | Warm, s | Attach, s | MB read from packs | Events | Host CPU, cold, s |
| --- | --- | --- | ---: | --- | --- | --- |
| `nbd:demand` (today, S12 backend, local packs) | 0.47 / 0.31 / 1.81 | 0.18 / 0.12 / 0.68 | 0.13 | 4.4 / 2.0 / 10.9 | – | 0.74 / 0.61 / 2.12 |
| `fan:demand` (unified file) | 0.60 / 0.35 / 2.02 | 0.18 / 0.12 / 0.63 | 0.11 | 4.0 / 1.2 / 9.8 | 1,432 / 460 / 4,385 | 0.76 / 0.53 / 2.19 |
| `fan:demand+f` (filled ranges answered on the reader thread) | 0.54 / 0.34 / 1.82 | 0.18 / 0.12 / 0.66 | 0.10 | 4.0 / 1.2 / 9.8 | 1,444 / 460 / 4,395 | 0.69 / 0.52 / 2.09 |
| **`fan:multidev+f`** (per-blob files, `-o device=`) | **0.45 / 0.31 / 1.50** | 0.18 / 0.12 / 0.67 | 0.14 | 4.0 / 1.2 / 9.8 | 144 / 47 / 700 | 0.69 / 0.53 / 1.77 |
| `nbd:readaround` (S12's 1 MiB read-around) | 0.55 / 0.31 / 1.48 | 0.18 / 0.12 / 0.62 | 0.13 | 22.9 / 11.2 / 40.2 | – | 1.33 / 0.87 / 2.72 |
| `fan:window` (each fill widened to its aligned 1 MiB) | 0.76 / 0.40 / 2.19 | 0.18 / 0.12 / 0.69 | 0.10 | 12.3 / 3.9 / 32.9 | 1,406 / 460 / 4,406 | 0.94 / 0.57 / 2.50 |

- **Multi-device matches or beats NBD:** 0.96× / 0.98× / 0.83× of `nbd:demand`, within S10's
  0.36–0.56 s for `import sys`. The unified file is 1.12–1.27× slower, because each 4 KiB page is
  one event (gate 1).
- **Event latency in Python** (p50, sequential): 20–70 µs on the unified file, and 30–260 µs on
  multi-device, where each event fills more. A native daemon would cut this.
- **Read amplification:** fanotify reads exactly the chunks a range overlaps: 10–40% fewer bytes
  than NBD, whose kernel readahead requests more blocks. Widening fills to 1 MiB (`fan:window`)
  only adds bytes from a local store.
- **Warm runs are the same on every path**: everything is in the page cache.

### Cold bursts over distinct images

N sandboxes start at once on one cold node, each with a different image from S12's
`burst-images.json`. Each runs `import sys`, then `pip --version`. Images without pip fail the
second command on every path (1 of 20, 7 of 100).

| Path | N | Wall, s | Attach median, s | First command done, median / max, s | `pip` median, s | Host busy CPU, s | Daemon CPU, s | MB read | Events (p50 latency) |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |
| `nbd:demand` | 20 | 7.2 / 7.1 | 1.55 / 1.65 | 2.81 / 3.22, 2.69 / 2.98 | 3.18 / 3.52 | 49 / 50 | 10.6 / 10.9 | 74 | – |
| `fan:demand` | 20 | 20.0 / 20.9 | 0.80 / 0.68 | 4.80 / 7.35, 5.60 / 9.78 | 14.9 / 14.8 | 150 / 171 | 40.0 / 42.1 | 63 | 102k (3.3 / 2.9 ms) |
| `fan:demand+f` | 20 | 7.9 | 0.79 | 2.38 / 3.11 | 5.36 | 84 | 12.1 | 63 | 102k (0.45 ms) |
| **`fan:multidev`** | 20 | **6.2** | 2.36 | 3.58 / 3.85 | 2.10 | 45 | 9.4 | 63 | 13.6k (1.9 ms) |
| **`fan:multidev+f`** | 20 | **5.6** | 2.32 | 3.17 / 3.40 | **1.98** | 43 | 8.4 | 63 | 13.8k (1.2 ms) |
| `nbd:readaround` | 20 | 11.5 | 1.66 | 4.36 / 6.21 | 6.42 | 64 | 22.5 | 281 | – |
| `fan:window` | 20 | 22.8 | 0.83 | 6.57 / 10.73 | 15.7 | 171 | 45.8 | 207 | 102k (2.7 ms) |
| `nbd:demand` | 100 | **27.6** | 8.48 | 16.9 / 18.8 | 10.3 | 203 | 43.8 | 202 | – |
| `fan:demand` | 100 | 85.8 | 2.76 | 28.9 / 45.1 | 55.7 | 2,870 | 234 | 164 | 502k (11.8 ms) |
| `fan:demand+f` | 100 | 101.6 | 2.88 | 33.5 / 54.1 | 66.9 | 3,416 | 123 | 164 | 502k (7.4 ms) |
| `fan:multidev` | 100 | 36.8 | 18.3 | 23.4 / 29.8 | 12.2 | 552 | 64.2 | 164 | 67k (15 ms) |
| `fan:multidev+f` | 100 | 37.3 | 18.0 | 25.5 / 28.3 | 11.5 | 580 | 56.8 | 164 | 66k (9.2 ms) |
| `nbd:readaround` | 100 | 46.2 | 7.39 | 24.8 / 31.5 | 18.3 | 285 | 85.9 | 719 | – |
| `fan:window` | 100 | 95.9 | 2.47 | 32.0 / 50.0 | 61.4 | 3,148 | 253 | 521 | 502k (12 ms) |

- **At 20-way, multi-device beats NBD:** 5.6–6.2 s against 7.1–7.2 s, with the same host CPU, a
  lower daemon CPU, and `pip` at 2.0 s against 3.2–3.5 s. The first command is slower (3.2–3.6
  against 2.7–2.8 s), because attach is slower. The attach cost is Python parsing each bootstrap's
  device and chunk tables under contention, not the kernel; the mount itself takes about 5 ms.
- **At 100-way, NBD wins:** 27.6 s against 37 s. The Python daemon saturates (daemon CPU 57–64 s
  against NBD's 44 s, event latency p50 9–15 ms) and attach queues behind it (18 s median).
- **The unified layout collapses under a burst:** 102k events at 20-way and 502k at 100-way.
  Python then takes 3–12 ms per event, and gVisor's systrap platform spins while it waits: 2,870
  CPU-seconds of host CPU at 100-way against NBD's 203. Answering filled ranges on the reader
  thread (`+f`) fixes 20-way (7.9 s) but not 100-way, where the single reader thread becomes the
  bottleneck.
- **Bytes:** fanotify reads 15–20% fewer pack bytes than `nbd:demand` (63 against 74 MB, and 164
  against 202 MB).
- **Against S11's fscache numbers.** S11 used 20 other images on a ccx43, measuring bytes at the
  registry, so this comparison is indicative only. There, a 20-way burst took 4.0 s wall, 38 CPU-s,
  a 1.24–1.29 s first-command median, a 0.24–0.27 s attach median and 1.73–1.79 s `pip`.
  `fan:multidev+f` gets within 1.4× of the wall time and matches the CPU and `pip`. Its first
  command lags (3.2 s) because of the Python attach.
- **The first multi-device burst served zeros.** The first `fan:multidev` runs
  ([`raw/burst-20-fan-multidev.json`](raw/burst-20-fan-multidev.json) and the `-100-` files)
  had 1 of 20 and 3–5 of 100 sandboxes fail beyond NBD's, with "source code string cannot contain
  null bytes". A shared blob file had been given only
  its first image's chunk entries. A later image used a chunk of the same blob that the first image
  shadowed, the daemon found no entry for that range, answered `FAN_ALLOW`, and EROFS read zeros.
  After the fix (the union of every attached image's entries, the `-fix` files), the failures equal
  NBD's. This is gate 2's fail-open hazard in another form: any range the daemon cannot place is
  silently zero. A shared blob file needs the layer's complete chunk table, and an unknown range
  should be denied.

## Gate 4: disk sharing across images

**Both work.** Reflink from a shared chunk cache brings disk use per extra image down to about a
tenth of per-image copies. Per-blob files share whole layers. ([`scripts/gate4.py`](scripts/gate4.py),
[`raw/gate4-copy.json`](raw/gate4-copy.json), [`raw/gate4-reflink.json`](raw/gate4-reflink.json),
[`raw/gate4-multidev.json`](raw/gate4-multidev.json), [`raw/gate4-align.json`](raw/gate4-align.json))

The first 24 images of S12's page-cache set (11 TMax and 13 Terminal-Lego on 4 shared bases) were
hydrated one after another on a fresh 400 GB XFS (`reflink=1`, `rmapbt=1`), with every file read
(11.1 GB of file data in total). After each image: `sync`, then the filesystem's used-bytes delta.
All 24 images in every mode matched the copy run's sha256 for every file.

| Mode | First image | Extra image, median / mean | Total, 24 images | Fill (full read) time, sum | Events |
| --- | ---: | --- | ---: | ---: | ---: |
| (i-a) per-image unified file, `pwrite` (gate 3's `fan:demand`) | 166 MB | 299 / 477 MB | 11.15 GB | 212 s | 1.11M |
| **(i-b) per-image unified file, `FICLONERANGE` from a chunk cache on the same XFS** | 171 MB | **29 / 202 MB** | **4.81 GB** | 369 s | 1.11M |
| (ii) per-image bootstrap + per-blob files shared across images, `-o device=` | 166 MB | 209 / 378 MB | 8.87 GB | **119 s** | 0.33M |

- **(i-b) Reflink.** The chunk cache is one file per chunk id, written once after verification
  (111,666 files) and cloned 284,339 times. An image whose chunks are all cached costs 1–30 MB: its
  bootstrap, its own new chunks and XFS extent metadata (for example 1.8 MB for pc-019 and 2 MB for
  pc-008). This approaches today's NBD chunk cache, which holds each chunk once and nothing per
  image.
  - **Cost:** the Python prototype fills at about half the speed of `pwrite` (369 against 212 s for
    the same reads), because it creates a cache file and makes one ioctl per chunk.
  - **Coherence:** XFS's remap path flushes and unmaps the destination range
    (`xfs_flush_unmap_range`), so stale zero pages from the backing file's readahead were never
    seen. 0 mismatches.
- **(ii) Multi-device in file-backed mode works** (`-o device=` paths are opened with `filp_open`
  when the primary device is a file). It shares only identical blobs: images on the same base layer
  share it, and everything above is per image. It is the fastest to fill, because its events are up
  to 128 KiB (gate 1). The two combine: per-blob files filled by reflink.
- **Alignment** (`FICLONERANGE` on XFS):

  | Clone | Result |
  | --- | --- |
  | 256 KiB slot of a slot file → 4 KiB-aligned destination, 256 KiB | ok |
  | slot → destination, length 100,000 (not a 4 KiB multiple, not at the source's EOF) | EINVAL |
  | slot → destination, length rounded up to 4 KiB (102,400) | ok |
  | per-chunk file of 100,000 B, length 0 or 100,000 (to the source's EOF) → middle of destination | **EINVAL** |
  | the same per-chunk file → the destination's EOF | ok |
  | per-chunk file zero-padded to 102,400 B, length 102,400 → middle of destination | ok |
  | destination offset not 4 KiB-aligned | EINVAL |

  - **Chunks and destinations line up.** Chunks start on 4 KiB boundaries in RAFS blob space, so
    destination offsets are always aligned, and 256 KiB slots work.
  - **The tail must be padded.** XFS refuses to clone a partial EOF block into the middle of a
    file, so a cache must store each chunk zero-padded to 4 KiB (as `fand.py` does) or use 256 KiB
    slots. The padding lands in the unused tail of the chunk's last block, which the copy path leaves
    as a hole (zeros).

## Gate 5: page-cache sharing (S12's item e)

**`inode_share` works on file-backed mounts and is the clear winner: 3.1 MB of page cache per extra
distinct image, against 21 MB for per-image mounts and 12 MB for per-layer stacks, all with
`-o directio`** ([`scripts/pcfan.py`](scripts/pcfan.py), [`raw/pagecache-*.json`](raw/),
[`raw/pc-prep.json`](raw/pc-prep.json)).

**Set and method** (S12's harness, ported to file-backed mounts):
- **Images:** the 25 of S12's 64 page-cache images that have `python3` (11 TMax and 14
  Terminal-Lego, on 4 shared bases).
- **Per variant:** no mounts and dropped caches at the start. Images then attach one after
  another; each runs S12's command (`import json, sqlite3, ssl, unittest, email, http.client,
  asyncio, decimal, argparse, logging`) once in runsc and stays mounted.
- **Metric:** after each image, `/proc/meminfo`. The table gives the least-squares slope of
  `Cached` over images 2–25. `MemAvailable` moved less than `Cached`, because page cache counts as
  available, so `Cached` is the honest measure. Slab grew under 1 MB per image everywhere.
- **Module:** every variant ran on the rebuilt `erofs.ko` with `CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y`.
  Its sha256 `2dde3450…babd7b6` equals S12's build, so the recipe is reproducible. It has
  vermagic `7.0.0-30-generic SMP preempt mod_unload modversions` and 26 `ishare` symbols, and it
  taints the kernel (unsigned, out of tree).
- **mkfs.erofs images** (the `erofs` and `ishare` variants): built with mkfs.erofs 1.9
  `--xattr-inode-digest=trusted.erofs.fingerprint --preserve-mtime` from each image's RAFS tree, as
  S12 planned. They are complete files, not filled on demand; 1,500 sampled files per variant
  matched the RAFS tree.

| Variant | Backing reads | First image, MB | **Per extra image, MB** | 25 images, MB | Attach median | Command median |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| per-image merged RAFS (`fand`, unified file) | buffered | 110 | 39.9 | 1,075 | 36 ms | 0.92 s |
| per-image merged RAFS | `-o directio`, `O_DIRECT` fills | 91 | **21.0** | 601 | 35 ms | 1.22 s |
| per-image RAFS, per-blob files shared (`multidev`) | buffered | 111 | 30.0 | 833 | 54 ms | 0.55 s |
| per-layer RAFS mounts stacked with OverlayFS (63 layer mounts) | buffered | 111 | 21.2 | 619 | 35 ms | 0.73 s |
| per-layer RAFS mounts stacked | `-o directio` | 92 | **12.3** | 390 | 34 ms | 0.91 s |
| per-image mkfs.erofs, no `inode_share` (control) | buffered | 89 | 35.9 | 952 | 8 ms | 0.70 s |
| per-image mkfs.erofs, no `inode_share` | `-o directio` | 70 | 18.0 | 503 | 8 ms | 0.67 s |
| per-image mkfs.erofs, `-o inode_share,domain_id=s13` | buffered | 89 | 5.7 | 245 | 8 ms | 0.62 s |
| **per-image mkfs.erofs, `inode_share`** | `-o directio` | 68 | **3.1** | **157** | 8 ms | 0.59 s |

**Reading the table:**
- **Buffered file-backed mounts cache everything twice:** once in EROFS's page cache, once in the
  backing file's. `-o directio` (and, for the fanotify path, `O_DIRECT` fills, `fand.py --odirect`)
  halves the cost on every layout. File-backed workers should always use it.
- **Per-layer stacking shares the base layers' pages:** 12.3 MB against 21.0 MB per image. That
  saving brings back OverlayFS stacks of many lowers (63 layer mounts for 25 images), and whiteout
  handling for RAFS layers.
- **`inode_share` shares identical files across all images,** whatever their layer: 3.1 MB per
  extra image, 6.8× less than per-image mounts and 4× less than per-layer stacks, with a single
  mount per image. The dmesg line "EXPERIMENTAL EROFS page cache share support in use" appears at
  every mount.
- **Command times are not comparable across the two halves of the table.** The mkfs.erofs images
  were complete files (0.59–0.70 s), while the RAFS variants include their on-demand fills.

**`inode_share` facts** (`fs/erofs/ishare.c`, `xattr.c`, `super.c`, `internal.h`):
- **It works on file-backed mounts.** `erofs_get_aops` picks `erofs_fileio_aops` for the real
  inode, and the shared inode reads through any one member inode (`erofs_real_inode`), that is,
  through some image's backing file. Inodes share only when their address-space operations are
  equal, so file-backed and block-backed images never share with each other.
- **What the kernel expects:**
  - **Superblock:** compat bit `0x20` (`COMPAT_ISHARE_XATTRS`), plus `ishare_xattr_prefix_id`
    (u8 at offset 105), which indexes the long xattr-name prefix table (`INCOMPAT_XATTR_PREFIXES`
    0x40). mkfs.erofs wrote compat 0x37, incompat 0x40, 1 prefix and id 0.
  - **Name:** the fingerprint is the xattr whose full name equals that prefix. The builder chooses
    the name; S13 used `trusted.erofs.fingerprint`.
  - **Value:** opaque bytes, at most one block. mkfs.erofs 1.9 writes 39 bytes: the ASCII
    `sha256:` followed by the 32-byte sha256 of the file content (for example
    `0x7368613235363a4e8bfbdf3b…` for a file whose sha256 is `4e8bfbdf3b…`).
  - **Matching:** the kernel appends the mount's `domain_id`, hashes the result with xxh32 for
    `iget5_locked`, and compares the whole value. A file shares only if its size also matches.
- **Restrictions:**
  - `domain_id` is required;
  - not with DAX;
  - `O_DIRECT` opens of shared files fail with EINVAL (`erofs_ishare_file_open`);
  - images without the on-disk feature mount with "on-disk ishare xattrs not found. Turning off
    inode_share."
  - It is marked experimental, and it excludes `EROFS_FS_ONDEMAND`.
- **With fanotify filling:** untested. A shared page may be read through another image's backing
  file, so each image's file must be filled independently; the daemon handles that, since events
  name the file. The mkfs.erofs image must keep its metadata complete and its data in `--blobdev`
  files (gate 1).

**What it implies for the converter.** RAFS from nydus-image v2.4.5 cannot carry fingerprints: it
has no prefix table, and its inodes have no room for a per-file xattr without a metadata relayout
(S12). Page-cache sharing therefore needs one of these:
- **Build images with mkfs.erofs 1.9**, for example per image from the OCI layers with `--tar`,
  `--chunksize=262144 --blobdev=<data>` and `--xattr-inode-digest=…`. The chunk map would then come
  from the image's chunk indexes and the sha256 of each 256 KiB data chunk, instead of from
  nydus-image's chunk table. This also avoids the `--repeatable` owner loss (side finding).
- **Teach our own converter to emit the prefix table and the xattr.**
- **Keep RAFS and stack per-layer mounts:** about 60% of the per-image cost, but none of
  `inode_share`'s sharing.

**Recommended mount granularity:**
- **M1:** per-image merged mounts, as the design says. On file-backed workers use `-o directio`.
  The NBD path should not double-cache file data, since EROFS reads data blocks straight into its
  own page cache, but S13 did not measure it.
- **Later, when we accept the experimental out-of-tree module** (or Ubuntu enables it):
  `inode_share` in one domain per node, with mkfs.erofs-built, fingerprinted images. Per-layer
  stacking is not worth its complexity next to that.

## Side finding: `nydus-image --repeatable` drops file owners

The full-tree check of gate 3 found that every non-root owner in all 7 images reads back as 0:0,
through NBD and fanotify alike. A one-layer test ([`scripts/uidcheck.sh`](scripts/uidcheck.sh),
[`raw/uidcheck.txt`](raw/uidcheck.txt)) isolates it: nydus-image v2.4.5 `create -t targz-rafs`
keeps `1000:42` and `0:101` without `--repeatable` and writes `0:0` with it; mkfs.erofs 1.9 `--tar`
keeps them. The design (§3 step 2) and `chunk_convert.py` pass `--repeatable` for C2.14's "same
task, same root". M1's full-tree gate (which compares owners) would fail every image with a
non-root file (`/etc/shadow` group 42, `/home/<user>`, `ssh-agent` group 101, …). The converter has
to drop `--repeatable` and get determinism another way (for example by normalising the bootstrap's
build time), or patch nydus-image. S10 compared sizes, modes and hashes only, which is why it did not
see this.

## Files

- **[`raw/`](raw/)**, copied from the VM during the run:
  - **Kernel:** `kernel-facts.txt` (uname, config, sysctls, header) and `kernel-source-notes.txt`
    (the cited source passages).
  - **Corpus:** `packer-main.json`, `packer-pc.json`, `pc-selection.json`, and the prep logs
    (`fetch.log`, `convert*.log`, `packer-*.log`, `select-pc.log`).
  - **Gate 1:** `gate1.json`, `gate1.log`.
  - **Gate 2:** `gate2.json`, `gate2.log`, `gate2b.log` (the takeover rerun),
    `gate2-takeover-deadlock.txt`, and `nbd-death.json` (the NBD control).
  - **Gate 3:**
    - `tree.json` (both paths, with timings) and `tree-owners.json` (fanotify only, with owner-aware
      classification);
    - `seq-main.json` (`nbd:demand`, `fan:demand`, `nbd:readaround`, `fan:window`) and
      `seq-fast.json` (`fan:multidev+f`, `fan:demand+f`);
    - `burst-<N>-<path>.json` for every burst. The `burst-*-fan-multidev*.json` files without
      `-fix` are the runs with the shared-blob bug; the `-fix` files are the valid ones;
    - `mdsmoke.json` (the multi-device, `O_DIRECT` and unmark-when-full checks).
  - **Gate 4:** `gate4-align.json`, `gate4-copy.json`, `gate4-reflink.json`, `gate4-multidev.json`.
  - **Gate 5:** `pc-prep.json` (with fingerprint xattrs and superblock bytes),
    `pagecache-<variant>.json`, `build-ishare.log`.
  - **Side finding:** `uidcheck.txt`.
  - **Summary:** `summary.json`, produced by `scripts/summarize.py`.
- **[`scripts/`](scripts/)**, run on the VM unless marked. S10's `fetch.py`, `convert.py` and
  `rafs.py`, and S12's `packer.py`, `packfmt.py`, `s3nbd.py`, `coldrun.py` and `select_pc.py`, were
  used unchanged from their spike directories (S12's `s3nbd.py` imports `s3lib.py`, which made no
  S3 calls here).
  - **Setup:** `setup.sh`, `prep.sh`, `ksrc.sh`, `ksrc2.sh`, `build_erofs_ishare.sh`.
  - **fanotify:** `fan.py` (ctypes bindings), `fanl.py` (minimal listener), `fdholder.py`,
    `flatten.py`, `common.py`.
  - **Gates:** `gate1.py`, `gate2.py`, `nbddeath.py`; `fand.py` (the chunk-store fill daemon) and
    `coldfan.py` (tree, seq and burst harness, run by `run_gate3.sh`, `run_rest*.sh`);
    `mdsmoke.py`; `gate4.py`; `pcfan.py`; `uidcheck.sh`.
  - **Summary:** `summarize.py`, run locally.

  These are spike code, not product code.
