# RL-scale qualification (2026-10-02)

This qualifies four in-flight items of
[`rl-scale-architecture-plan.md`](../../rl-scale-architecture-plan.md) on real
kernels, devices and gVisor:

* the EROFS metadata walker on erofs-utils 1.9 (C2.2);
* the NBD/EROFS path with metadata hints and startup traces (C2.2, C2.3);
* pause, reclaim and thaw of live gVisor sandboxes (C1.1);
* host-visible Unix sockets for an in-guest agent (S7, C5.1).

**Host.** The disposable Hetzner CPX62 of the
[M0 spikes](../rl-scale-spikes-2026-10-01/README.md): 16 shared AMD EPYC Genoa
vCPUs, 31 GiB RAM, Ubuntu 26.04.1, kernel **7.0.0-30-generic**. Disk is the
local non-rotational virtual disk. Swap is a 16 GiB swapfile on it with zswap (zstd,
`max_pool_percent=20`), `vm.page-cluster=3`. The work root is XFS with
reflink on a loop file. Tools: erofs-utils **1.9.1**, Docker 29.1.3
(overlay2), busybox-static 1.37. The host had public IPv4 and SSH from one
workstation only, and no production network, credential or service.

**runsc.** `release-20260817.0-dirty`, sha256 `0fc94760976301c2…`, the pinned
build with patches 0001–0005 at `/usr/local/libexec/ucloud-gvisor/`. It has no
`--application-memory-ram-backing` flag (patch 0006). RAM mode is therefore
approximated by `--application-memory-file-dir` on a tmpfs, which is how
patch 0006 backs memory too, but with `noswap` removed as C1.1 proposes.

**Workload images.** `/srv/spike/rootfs`: python:3.12-slim plus numpy, pandas
and scipy, 415 MiB, 13,537 entries (11,666 files, 1,372 directories, 507
symlinks).

## Summary

| Item | Result | Consequence |
| --- | --- | --- |
| C2.2 walker on erofs-utils 1.9 | **No walker bug.** All 55 walker, hint and prefetch tests pass on 1.9 as root, after one test fix: 1.9 adds an empty `trusted.overlay.origin` to every directory that holds a whiteout. On the scientific rootfs, built with the production flags with and without `--mkfs-time`, uncompressed, and with `--MZ`, no block outside the walker's ranges is read by a kernel mount (metadata-only and full-read traversals), `fsck.erofs --extract` or `dump.erofs`, apart from data extents. Overwriting everything else leaves the kernel-visible tree, every file's bytes and fsck unchanged. | The walker is qualified for 1.9 builders. |
| C2.2 hint on a real image | Metadata is 9.1 MB, but it is spread over **272 of 893** 256 KiB chunks (68 MiB). The 32 MiB default budget prefetches 128, so a `find` after attach still made 144 remote reads. Built with erofs-utils 1.9 `--MZ` (separate metadata zone, no new feature bit), the same metadata fits **37 chunks**: the hint is complete, attach waits 0.14 s at 20 ms per request, and **`find` makes zero remote reads**. | **New: build components with `--MZ`.** It needs erofs-utils 1.9 on builders, and a `layer_format` change so that zoned and unzoned components never share identity (alongside C2.11's timestamp mode). Workers need nothing. |
| C2.3 trace replay | On a fresh cache with only the first attach's trace, the first command (the scientific import) made **zero demand misses**. It took 2.6 s instead of 9.2 s (default image) or 11.2 s (`--MZ`) at 20 ms per request. | C2.3 works end to end through NBD, EROFS and the verified cache. |
| `qualify_environment.py` | Passes on 1.9 with `--layers`, and with the new `--prefetch`. That flag publishes hints, exports backend metrics, and re-attaches on a fresh cache with traces only: zero demand misses for the second live guest. | The qualification now covers C2.2/C2.3. |
| C1.1 pause and resume | `runsc pause` 9–26 ms. `runsc resume` 16–70 ms alone, up to 152 ms with eight sandboxes swapping at once. First `runsc exec true` 41–158 ms after resume, against 24 ms on a running sandbox. | The pause ≤ 50 ms gate passes. The thaw ≤ 100 ms gate passes for one sandbox. |
| C1.1 reclaim | With a target of all of `memory.current` (about 670 MiB) and `swappiness=200`, everything but about 7 MiB left RAM. With zswap off this took 1.8–3.0 s (225–380 MiB/s). With zswap on it took 5.6–10 s, because the kernel compresses every page and then writes it back to disk to meet the target. | **Disable zswap per paused cgroup** (`memory.zswap.max=0`) for full reclaim, or keep it only with `memory.zswap.writeback=0` (compressible data: 2.95× here, about 100 MiB/s of input). Do not combine a full target with writeback. |
| C1.1 refault | The guest faulting its 640 MiB heap back from disk swap took **3.6–5.0 s** (6.5–8.0 s with eight sandboxes). With zswap-only, compressible data took 2.2–2.6 s. **A host prefetch of the memory file with 8 threads before resume took 0.81–0.95 s.** After it, the guest's full touch took 52–67 ms, about warm speed. | **Thaw prefetch is mandatory.** Read back the memory file's SEEK_DATA extents in parallel from the Warden. The plan's `MADV_POPULATE_READ` on a shared mapping should behave the same on tmpfs (not measured). |
| C1.1 against hibernate | A stock checkpoint of the same sandbox wrote 645 MiB in 160–180 ms, into the page cache with no fsync. Restore took 0.27 s warm and 1.0–1.1 s with a cold page cache, and the restored memory is resident. | A pause plus prefetch thaw (about 1 s) costs about the same as a cold hibernate restore, and avoids capture, durability and upload. |
| C1.1 `runsc pause` and the cgroup | `runsc pause` does **not** freeze the cgroup (`frozen 0`). Freezing after it took < 1 ms and did not change reclaim. | Correct the plan's "runsc pause, which freezes the cgroup". |
| S7 / C5.1 | `create`: a guest listener in a bind-mounted host directory is a real host socket, and a host process round-trips through it. `open`: a guest connects to a host listener. `all` allows both. `none` allows neither. A sandbox holding a host-bound listener **cannot be checkpointed and is destroyed** by the attempt (stock and `--hibernate`). A guest connection to a host listener checkpoints and restores. | **C5.1: the agent dials out** (`--host-uds=open`, the Warden listens), or closes its listener before every capture. A failed capture kills the sandbox. |

## 1. EROFS metadata walker on erofs-utils 1.9 (C2.2)

### Unit tests

`tests.test_erofs_metadata`, `tests.test_environment_metadata` and
`tests.test_environment_prefetch` were run as root on the host
([`walker-tests-root.log.gz`](walker-tests-root.log.gz)). The first run had one
failure, and it was in the test, not the walker. erofs-utils 1.9 gives every
directory that directly holds a whiteout (a 0:0 character device) an empty
`trusted.overlay.origin`; 1.4 does not. Overlayfs takes an origin xattr on a lower
directory as a sign that it may contain whiteouts, and so filters them in
readdir even where the directory is not merged (from the kernel's overlayfs
design; not separately tested here). [`mz-and-xattr-probes.json.gz`](mz-and-xattr-probes.json.gz) shows the rule:
it applies to the root, to nested and to opaque directories, and
`--ovlfs-strip=1` drops it together with the opaque xattrs (and, per its
help text, the whiteouts). The walker
already covered the xattr: the overwritten-image reader returned it. The test
now accepts exactly this xattr, empty, on exactly those directories.
`-Elegacy-compress` still produces full (layout 1) indexes on 1.9, and the
chunked and big-pcluster variants keep their layouts. All 55 tests pass on 1.9,
and the walker tests also pass locally on 1.4.

### The scientific rootfs with production flags

[`walker-rootfs-proof.py`](walker-rootfs-proof.py) builds each variant twice
(determinism), walks it, and then proves completeness at 4 KiB block
granularity. Every block a reader touches, minus the regular-file data extents
`dump.erofs -e` reports, must be in the walker's ranges. The readers are:

* the **kernel**, mounting the image over the repository's NBD export with a
  logging cache: a metadata-only traversal (readdir, `lstat`, `readlink`,
  `listxattr`, `getxattr`, `statfs`), and a full read of every file;
* `fsck.erofs --extract` and `dump.erofs --nid -e` on every directory and
  symlink plus 1/16 of the other inodes, both traced with strace.

Finally, every byte outside the metadata ranges and data extents is overwritten
with `0x5a`. A kernel mount of the result must show an identical tree: modes,
owners, sizes, link counts, device numbers, symlink targets, xattrs, and the
SHA-256 of every file. `fsck` must still pass, and the walker must return the
same ranges.

Production flags are `-T 0 -U 00000000-0000-0000-0000-000000000000 -zlz4
--exclude-regex=^(dev|proc|run|sys)$`.

| Variant | Image MiB | Inodes | Metadata MB | Metadata blocks | 256 KiB hint chunks | Walk s | Rebuild identical |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| production `-zlz4` (`-T0`) | 222.8 | 13,536 | 8.70 | 2,173 | 273 | 0.08 | yes |
| production + `--mkfs-time` (C2.11) | 223.2 | 13,536 | 9.07 | 2,268 | 272 | 0.11 | yes |
| `--mkfs-time`, uncompressed | 383.3 | 13,536 | 20.05 | 4,961 | 420 | 0.08 | yes |
| production + `--mkfs-time --MZ` | 223.2 | 13,536 | 9.07 | 2,268 | **37** | 0.09 | yes |

| Variant | Kernel metadata traversal | Kernel full read | `fsck --extract` | `dump.erofs` sample | Overwritten image |
| --- | --- | --- | --- | --- | --- |
| production `-zlz4` | 2,167 reads, 0 uncovered | 223 MiB read, 0 uncovered | 89,369 reads, 0 uncovered | 2,599 nids, 0 uncovered | tree and bytes identical, fsck ok |
| + `--mkfs-time` | 2,262 reads, 0 uncovered | 223 MiB, 0 uncovered | 89,579 reads, 0 uncovered | 2,599 nids, 0 uncovered | identical, fsck ok |
| uncompressed | 4,958 reads, 0 uncovered | 383 MiB, 0 uncovered | 34,739 reads, 0 uncovered | 2,599 nids, 0 uncovered | identical, fsck ok |
| + `--MZ` | 2,262 reads, 0 uncovered | 223 MiB, 0 uncovered | 89,637 reads, 0 uncovered | 2,599 nids, 0 uncovered | identical, fsck ok |

The metadata-only kernel traversal never read a block outside the walker's
ranges, even before data extents were subtracted. The walk itself takes about
0.1 s for 13.5k inodes.

**Metadata placement.** Without `--MZ`, mkfs interleaves inodes and
directory blocks with file data. A 9 MB metadata set then touches 30% of the
image's chunks. With 1.9's `--MZ`, the same metadata sits in 37 chunks
([`mz-and-xattr-probes.json.gz`](mz-and-xattr-probes.json.gz)). The superblock
feature bits stay `compat=0x3 incompat=0x1`, so no newer kernel or walker
feature is needed. `--MZ=i` (inodes only) gives 44 chunks; `--MZ=d` alone is
rejected by mkfs.

## 2. Real NBD/EROFS path (C2.2, C2.3)

### `qualify_environment.py`

```sh
PYTHONPATH=. .venv/bin/python runtime/storage_native/qualify_environment.py \
  --runsc /usr/local/libexec/ucloud-gvisor/runsc --layers [--prefetch] --output out.json
```

Both runs passed, including the `--layers` scenario (3 images over real Docker
overlay2 diffs, 5 component references, 3 distinct components, live guest and
copy-up, shared-base GC fence, zero-upload republication). Raw results:
[`qualify-environment-base.json.gz`](qualify-environment-base.json.gz) and
[`qualify-environment-prefetch.json.gz`](qualify-environment-prefetch.json.gz).

| Fixture (base 69.3 MB + toolkit 4 KiB) | Base run | `--prefetch` first attach | `--prefetch` replay, fresh cache |
| --- | ---: | ---: | ---: |
| Metadata hint | none | base 3 chunks (9,061 metadata bytes), toolkit 1 | same |
| Cold materialize (frontend: pull, compose, prepare) | 0.52 s | 0.42 s | 0.43 s |
| Blob bytes fetched by then | 294,211 | 658,755 | 2,493,763 |
| Prefetched at attach | — | metadata 4 chunks (616 KiB, 16 ms) | metadata 4 + trace 7 chunks (1.75 MiB, 28 ms) |
| Guest ready (runsc run to first output) | 0.15 s | 0.30 s (0.15 s in an earlier run) | 0.15 s |
| Warm re-materialize (frontend replacement) | 0.29 s, 0 bytes | 0.30 s, 0 bytes | — |
| Demand chunk misses through the guest | — | 7 | **0** |
| Total blob bytes / image bytes | 2,391,363 / 69,312,512 (3.4%) | 2,493,763 (3.6%) | 2,493,763 |

The fixture registry is in-process, so it shows correctness and byte
accounting, not latency. The traces were recorded in the first attach's 8 s
window (10 chunks over the two components) and copied to the replay's fresh
backend root.

### A real image: the scientific rootfs

[`prefetch-rootfs-bench.py`](prefetch-rootfs-bench.py) publishes the rootfs
twice (with and without its signed hint) to a fixture HTTP registry that adds a
fixed delay per blob request. It then runs the metered backend on a fresh root
and cache, with the page cache dropped. From separate processes it times
`ensure` (attach, mount and the bounded metadata wait), a full `find -xdev`
walk, and the first command: a chroot `python3 -c "import numpy, pandas,
scipy.linalg, scipy.sparse, scipy.stats, scipy.optimize"`. Images use the C2.11
flags (`--mkfs-time`), so no `.pyc` is rewritten. Raw results:
[`prefetch-rootfs.json.gz`](prefetch-rootfs.json.gz) and
[`prefetch-rootfs-mz.json.gz`](prefetch-rootfs-mz.json.gz).

| Image | Delay | Mode | ensure s | prefetched at ensure | find s | find remote reads | import s | import remote reads | demand misses |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default (272 hint chunks) | 0 ms | plain | 0.05 | — | 2.30 | 271 | 3.47 | 270 | 542 |
| | 0 ms | hint | 0.20 | 128 chunks, 32 MiB (budget) | 2.09 | 144 | 3.78 | 270 | 414 |
| | 0 ms | hint + trace | 0.25 | + 414 trace chunks | 1.87 | (replay) | 2.57 | 0 | **0** |
| | 20 ms | plain | 0.11 | — | 8.07 | 271 | 9.21 | 270 | 542 |
| | 20 ms | hint | 0.92 | 128 chunks, 32 MiB (budget) | 5.11 | 144 | 8.95 | 270 | 414 |
| | 20 ms | hint + trace | 0.95 | + 414 trace chunks | 2.13 | (replay) | 2.60 | 0 | **0** |
| `--MZ` (37 hint chunks) | 0 ms | plain | 0.06 | — | 1.67 | 35 | 3.61 | 372 | 409 |
| | 0 ms | hint | 0.08 | 37 chunks, 9.2 MiB (complete) | 1.19 | **0** | 3.78 | 372 | 372 |
| | 0 ms | hint + trace | 0.08 | + 372 trace chunks | 1.96 | (replay) | 2.84 | 0 | **0** |
| | 20 ms | plain | 0.12 | — | 2.40 | 35 | 11.15 | 372 | 409 |
| | 20 ms | hint | 0.14 | 37 chunks, 9.2 MiB (complete) | 1.50 | **0** | 11.33 | 372 | 372 |
| | 20 ms | hint + trace | 0.14 | + 372 trace chunks | 2.01 | (replay) | 2.56 | 0 | **0** |

"(replay)" means the remote reads during `find` were the background trace
replay (37–99 requests), not demand reads. Each run read 542 chunks (135 MiB,
61% of the 223 MiB image) for the default layout and 409 chunks (102 MiB, 46%)
for `--MZ`. Clustering metadata stops metadata chunks from dragging in
unrelated file data. The trace window (20 s here) also recorded the `find`, so
these traces are larger than a pure startup trace.

**Conclusions.**

* On a default-layout image, the C2.2 gate (no remote reads during `find`
  after attach) fails at the default budget, and only partly improves. Raising
  `PrefetchPolicy.metadata_bytes` to cover 272 chunks would fetch 68 MiB to get
  9 MB of metadata. `--MZ` fixes the layout instead: one complete 9.2 MiB hint
  in 5 bulk requests, and the gate passes.
* The C2.3 replay turns the first command into a warm-cache run: 2.6 s instead
  of 9–11 s at 20 ms per request, with zero demand misses.
* At 20 ms, the hint-only attach of the default image waits 0.92 s for 128
  scattered chunks in 71 ranges; with `--MZ`, 0.14 s.

**Observation (not production-relevant).** An `os.walk` from inside the
process that serves the NBD export twice stalled for the kernel's 30 s request
timeout (one request was never answered, followed by EIO). The same traversal
from a child process never failed, nor did an in-process `lstat`. The artifact
backend never reads its own mounts, apart from one `stat` of a mount root,
which mount has already cached. The proof script therefore traverses from a
child process. This was not root-caused.

## 3. gVisor pause, reclaim and thaw (C1.1)

[`pause-tier-bench.py`](pause-tier-bench.py). Each sandbox runs from a host
overlay over the rootfs, in its own cgroup (`cgroupsPath`) under one cgroup the
script owns. It has `--network=none`, `--platform=systrap` and default
`--overlay2=root:self`, with application memory in a file on a swappable tmpfs
(`--application-memory-file-dir` plus the quota-owned directory annotation).
The guest is `python3` holding a 512 MiB heap and touching every page of a
128 MiB hot set in a loop. On SIGUSR1 it times one touch of every page of both
("refault"), then verifies the heap's SHA-256. The heap is random (`random`)
or text-like (`text`, 2.95× under zstd). Every sequence was verified intact.

Per sandbox, the cycle is:

1. `runsc pause`;
2. one `memory.reclaim` write of `"<memory.current> swappiness=200"`;
3. optionally, a parallel host read of the memory file;
4. `runsc resume`;
5. the first successful `runsc exec <id> true`;
6. the refault.

For eight sandboxes, all steps run concurrently. A running sandbox's
`memory.current` was about 670 MiB: 645 MiB of tmpfs memory file and 15 MiB of
Sentry and gofer anonymous memory. Its warm full touch took 28 ms (13–34) and
`runsc exec true` 24 ms (15–31). Values are median (min–max).

| Heap | Mode | n | zswap | runs | pause ms | reclaim s | RAM freed MiB | MiB/s per sandbox | zswap pool after MiB | resume ms | prefetch s | first exec after resume ms | guest refault s |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| random | pause | 1 | off | 3 | 17 (9–19) | 2.93 (1.78–3.02) | 679 | 232 | 0 | 46 (43–47) | — | 112 (86–121) | 3.81 (3.62–4.20) |
| random | pause | 1 | on | 3 | 18 (13–20) | 5.88 (5.76–7.28) | 681 | 118 | 0 | 54 (44–70) | — | 103 (96–158) | 4.67 (4.44–4.76) |
| random | pause + cgroup.freeze | 1 | on | 2 | 15 | 5.78 (5.56–6.00) | 678 | 118 | 0 | 34 (31–38) | — | 87 (77–98) | 4.71 (4.41–5.00) |
| random | pause + prefetch | 1 | off | 2 | 18 (17–18) | 2.04 (1.98–2.10) | 660 | 324 | 0 | 41 (39–43) | **0.83 (0.81–0.85)** | 76 (73–80) | **0.07** |
| random | pause + prefetch | 1 | on | 1 | 18 | 6.44 | 660 | 103 | 0 | 48 | 0.95 | 86 | 0.05 |
| text | pause | 1 | off | 1 | 18 | 2.00 | 664 | 332 | 0 | 42 | — | 92 | 5.34 |
| text | pause | 1 | on | 2 | 12 (12–13) | 9.68 (9.34–10.02) | 663 | 69 | 0 | 43 (41–45) | — | 98 (86–109) | 5.95 (5.84–6.05) |
| text | zswap only (`writeback=0`) | 1 | on | 2 | 18 (16–20) | 6.80 (6.03–7.56) | 438 | 65 | 224 | 18 (16–20) | — | 41 | 2.30 (2.19–2.40) |
| text | pause + prefetch | 1 | on | 1 | 19 | 9.25 | 664 | 72 | 0 | 37 | 2.54 | 118 | 0.06 |
| random | pause | 8 | off | 8 | 16 (10–21) | 5.81 (3.08–6.32) | 660 | 114 | 0 | 55 (29–66) | — | 146 (125–187) | 6.76 (6.49–8.01) |
| random | pause | 8 | on | 8 | 16 (12–26) | 30.88 (28.32–31.05) | 660 | 21 | 0 | 106 (49–128) | — | 170 (118–345) | 6.76 (6.67–7.59) |
| random | pause + prefetch | 8 | off | 8 | 13 (8–21) | 5.16 (3.81–6.16) | 661 | 128 | 0 | 76 (51–152) | **3.24 (2.59–3.42)** | 127 (98–319) | **0.06** |
| text | pause | 8 | on | 8 | 17 (12–20) | 8.51 (7.36–9.76) | 439 | 51 | 223 | 35 (25–44) | — | 67 (58–88) | 2.49 (2.42–2.90) |
| text | zswap only (`writeback=0`) | 8 | on | 8 | 18 (10–23) | 8.56 (7.90–9.70) | 438 | 51 | 224 | 20 (18–32) | — | 37 (25–54) | 2.56 (2.40–2.78) |

Hibernate comparison: the same sandboxes, stock `runsc checkpoint
--image-path` then `runsc delete` and `runsc restore --detach`. The checkpoint
is written to the page cache with no fsync. "Cold" drops the page cache before
restore. Raw results: [`pause-tier.json.gz`](pause-tier.json.gz),
[`pause-tier-hibernate-cold.json.gz`](pause-tier-hibernate-cold.json.gz).

| n | page cache | runs | checkpoint ms | image MiB | restore ms | first exec ms | guest refault ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | warm | 3 | 161 (158–182) | 645 | 266 (266–268) | 25 (22–27) | 51 (51–54) |
| 1 | cold | 2 | 170 (163–176) | 645 | 1,046 (971–1,121) | 46 (45–47) | 48 (39–57) |
| 8 | warm | 8 | 774 (690–828) | 645 | 568 (467–620) | 22 (16–26) | 51 (45–65) |
| 8 | cold | 8 | 885 (196–983) | 645 | 1,920 (1,920–2,123) | 30 (26–33) | 37 (34–45) |

After restore, `memory.current` was about 2.0 GiB. That includes about 1.4 GiB
of reclaimable page cache charged while reading the image.

**Findings.**

* **Latency gates.** `runsc pause` (9–26 ms) passes ≤ 50 ms. `runsc resume`
  (16–70 ms alone) passes ≤ 100 ms; under eight concurrent swap-ins the tail
  reached 128–152 ms. The first exec after resume costs 41–158 ms, against
  24 ms running, because the Sentry's and gofer's own heaps (about 15 MiB)
  were reclaimed too, and memory-file prefetch does not bring them back.
* **`runsc pause` does not freeze the cgroup.** It stops guest tasks inside
  the Sentry. `cgroup.events` stayed `frozen 0`. Writing `cgroup.freeze=1`
  afterwards took < 1 ms and changed nothing measurable; it must be thawed
  before `runsc resume` or `exec`.
* **A full reclaim target moves everything.** `memory.reclaim` returned
  `EAGAIN` with about 7 MiB left. That is expected, and the caller must treat
  it as success. The kernel charges zswap's compressed pool to the cgroup, so
  with zswap on, a full target compresses every page (`zswpout`) and then
  writes it all back to disk (`zswpwb` = `zswpout`, `pswpout` 168k pages).
  That is 2–3× slower than zswap off and leaves nothing in zswap. With eight
  concurrent reclaims of random data it fell to 21 MiB/s per sandbox, against
  114 MiB/s with zswap off. Compression is CPU-bound at about 100 MiB/s of
  input per cgroup (zstd).
* **Policy options.**
  * Full reclaim to disk: zswap disabled for the sandbox's cgroup,
    225–380 MiB/s alone, about 0.85 GiB/s aggregate for eight.
  * zswap only (`memory.zswap.writeback=0`): frees (1 − 1/ratio) of
    compressible memory, nothing of incompressible memory, and refaults from
    RAM in 2.2–2.6 s for 640 MiB.
  * One case did not behave as the single-sandbox run: eight concurrent
    `text` reclaims with zswap and writeback on also kept their pool
    (223 MiB), so writeback did not run under that concurrency.
* **Refault dominates thaw unless it is prefetched.** The guest's own faults
  from swap run at 130–180 MiB/s: synchronous 4 KiB faults with 8-page
  readahead, 3.6–5.0 s for 640 MiB. A host read of the memory file's SEEK_DATA
  extents, 4 MiB pieces over 8 threads, swapped the same 645 MiB back in
  0.81–0.95 s, about 750 MiB/s. Swap-in is charged back to the sandbox's
  cgroup. After it, the guest's full touch took 50–70 ms. With eight
  sandboxes prefetching at once, each took 2.6–3.4 s, about 1.5 GiB/s
  aggregate. Text data written back from zswap prefetched more slowly
  (2.5 s), because writeback order scatters swap slots.
* **Pause against hibernate.** For a full-heap thaw, a pause plus prefetch
  (about 0.9 s) costs about the same as a cold-cache restore (1.0–1.1 s),
  without a 645 MiB capture, fsync or upload. Hibernate remains the tool for
  drain and long waits, as the plan says. A thaw that touches only the hot
  set refaults in proportion.

## 4. S7: Unix sockets across a bind-mounted host directory (C5.1)

[`s7-host-uds.py`](s7-host-uds.py). A python guest (network none, host-overlay
rootfs, default `--overlay2=root:self`) gets a host directory bind-mounted at
`/agent`. "Guest → host" means the guest binds and listens on
`/agent/guest.sock` and a host process connects to the host path. "Host →
guest" means a host process listens on `<dir>/host.sock` and the guest
connects to `/agent/host.sock`. Both round-trip bytes. Raw results:
[`s7-host-uds.json.gz`](s7-host-uds.json.gz).

| `--host-uds` | Guest bind in `/agent` | Host sees / connects | Guest connects to host listener | Guest bind in `/` (overlay rootfs) |
| --- | --- | --- | --- | --- |
| `none` | succeeds, sandbox-internal | nothing at the path | `ECONNREFUSED` | internal; nothing on the host |
| `open` | succeeds, sandbox-internal | nothing at the path | **works** | internal |
| `create` | succeeds, **a real host socket** | **socket; round trip works** | `ECONNREFUSED` | internal |
| `all` | real host socket | works | works | internal |

| Capture while the sandbox holds… | Stock `runsc checkpoint` | `runsc checkpoint --hibernate` (production path) |
| --- | --- | --- |
| a host-bound listener (`create`/`all`) | fails: `encoding error: Cannot save endpoint with bound host socket` (a Sentry panic in `connectionedEndpoint.beforeSave`); **the sandbox is left stopped** | same error; **the sandbox is left stopped** |
| a listener it closed first | succeeds; restore succeeds; the guest rebinds and the host connects again | succeeds (sandbox quiesced `paused`); `runsc resume`, rebind and connect work |
| a connection to a host listener (`open`) | succeeds; restore succeeds | succeeds; sandbox quiesced `paused` |

**Conclusions for C5.1.**

* A host-visible agent socket works in our mount layout, through a bind mount
  served by the gofer. It does not work through the overlay rootfs.
* A guest-side listener (`create`) is incompatible with capture. Every park,
  drain or fork would first have to close it, and a missed close destroys the
  sandbox, even on the `--hibernate` path that patch 0004 makes abortable.
* Prefer **dial-out**: run the sandbox with `--host-uds=open`, have the Warden
  listen on a per-sandbox socket in the bind-mounted directory, and have the
  agent connect, reconnecting after restore. Captures then succeed with the
  connection open. `open` also does not let the guest create host-visible
  sockets.

## 5. Code changes in this tranche

* `tests/test_erofs_metadata.py`:
  * accepts the empty `trusted.overlay.origin` that erofs-utils 1.9 adds to
    directories holding whiteouts, and nothing else;
  * adds a `--MZ` (metadata zone) image to both completeness proofs when mkfs
    supports it.
* `runtime/storage_native/qualify_environment.py`: `--prefetch` publishes
  signed metadata hints and runs the backend with exported metrics. It records
  hint, prefetch, trace and miss counters after materialization and after the
  guest. It then re-attaches on a fresh backend and cache that hold only the
  traces, with a second live guest. It requires every component to attach
  with its hint, a recorded trace per component, and a nonzero trace replay.
  Guest start and finish and mount cleanup are factored into helpers.
* `runtime/storage_native/qualify_environment_layers.py`: no
  `hashlib.file_digest`, which needs Python 3.11. The `--layers` scenario
  failed under the project's own Python 3.10 environment. `scripts/backup_relay_postgres.py`
  and `runtime/storage_native/benchmark_memory_tiers.py` still use it.

## Files

| File | Content |
| --- | --- |
| `walker-tests-root.log.gz` | Walker, hint and prefetch unit tests as root on erofs-utils 1.9 |
| `walker-rootfs.json.gz`, `walker-rootfs-mz.json.gz` | Rootfs walker proofs, per variant |
| `mz-and-xattr-probes.json.gz` | `--MZ` chunk counts and feature bits; the whiteout-directory xattr rule |
| `qualify-environment-base.json.gz`, `qualify-environment-prefetch.json.gz` | `qualify_environment.py --layers` with and without `--prefetch` |
| `prefetch-rootfs.json.gz`, `prefetch-rootfs-mz.json.gz` | Scientific rootfs: plain, hint, hint + trace at 0 and 20 ms |
| `pause-tier*.json.gz` | Pause, reclaim, prefetch, resume and hibernate runs (random, text, cold restore) |
| `s7-host-uds.json.gz` | S7 socket matrix and capture behaviour |
| `walker-rootfs-proof.py`, `prefetch-rootfs-bench.py`, `pause-tier-bench.py`, `s7-host-uds.py` | The scripts. They expect a checkout at `/root/qual/ucloud-sandboxes` and its `.venv`; run them as root on a disposable host only |

**Teardown.** After the runs, the server (168285131), its firewall
(11717713) and its temporary SSH key (130765472) were deleted through the
Hetzner Cloud API, and each now returns 404. The local private key was also
deleted. Nothing else in the project changed: the production gateway server,
both production firewalls and the production SSH key remain.
