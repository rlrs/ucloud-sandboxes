# RL-scale M0 spikes (2026-10-01)

These are results for spikes S1, S2, S4, S5, S6, S7 and S8 from
[`rl-scale-architecture-plan.md`](../../rl-scale-architecture-plan.md),
section 9.

**Host.** One disposable Hetzner CPX62 (16 shared AMD vCPU, 32 GiB) running
Ubuntu 26.04.1 with kernel **7.0.0-30-generic**, the kernel that production
workers pin. The host had public IPv4 only, no production network, and the
label `purpose=rl-scale-spike`.

**runsc.** `release-20260817.0-dirty`, sha256 `0fc94760976301c2…`. This is the
pinned release built with our patches (the 2026-09-23 qualification build),
installed at the production path `/usr/local/libexec/ucloud-gvisor/`.

**Work root.** XFS (`reflink=1`) on a loop file. Swap was a 16 GiB swapfile
with zswap/zstd. No production service, credential or network was involved.

## Results

| Spike | Question | Answer |
| --- | --- | --- |
| S1 | Default `--overlay2`, and where do guest writes land? | `root:self`. The 64 MiB the guest wrote appear only as 64 MiB allocated in `.gvisor.filestore.<cid>` inside the host upper. The guest's file is not visible on the host, and the rest of the upper is 0 bytes. Confirms plan D1: a hibernate must serialize everything written. |
| S2 | Does the gofer path duplicate image pages per Sentry? | **Only modestly.** Shared libraries and file data stay in the shared host page cache or the shared EROFS mapping. What grows per sandbox is the Sentry's own anonymous memory. See below. |
| S4 | Does `runsc tar rootfs-upper` exist and work? | Yes. On a running container it exported the guest's writes (7 members). C3.1 needs no new gVisor patch to export. |
| S5 | Can a checkpoint restore into a different netns, and more than once? | **Yes.** Each restored child takes its new netns address (10.200.2.2 and 10.200.3.2 from a 10.200.1.2 source), reaches its own gateway over TCP, and keeps the in-memory `/tmp` state. Two restores from one image both succeed; `runsc restore` does not consume the image. `/dev/urandom` differs between children. Busybox guest: checkpoint 62 ms, each restore about 115 ms. |
| S6 | Do frozen-cgroup `memory.reclaim swappiness=` writes push tmpfs memory to zswap/swap on kernel 7.0? | **Yes.** 2 GiB of tmpfs moved out in 8.06 s (incompressible) or 6.42 s (compressible). Thaw took 0 ms; faulting the full 2 GiB back sequentially took 3.25 s and 7.2 s. Integrity was intact. `cgroup.freeze`, `memory.reclaim` with the swappiness argument, `memory.zswap.max` and `memory.zswap.writeback` are present. |
| S7 | `--host-uds` values | `none` (default), `open`, `create` and `all`. |
| S8 | Kernel features | cgroup v2 with `cpu.idle`, `cpu.weight`, freeze and kill, and `memory.reclaim`; zswap built in (off by default); `CONFIG_SCHED_CORE=y`; EROFS, ublk and overlay are modules; XFS reflink on the work root. **`CONFIG_EROFS_FS_ONDEMAND` is not set**, so EROFS over fscache is unavailable on this kernel. |

### S2 page sharing

**Workload.** K ∈ {1, 8, 32} sandboxes from one rootfs: python:3.12-slim plus
numpy 2.5.3, pandas 3.0.6 and scipy 1.18.1, 414 MiB. Each sandbox ran
`python3 -c "import numpy, pandas, scipy.linalg, scipy.sparse, scipy.stats, scipy.optimize"`.

**Variants.**
- `gofer`: host overlay served by the gofer (production's path).
- `erofs`: Sentry-native EROFS rootfs through the `dev.gvisor.spec.rootfs`
  annotations.

The table uses the second run (`report-s2-mtime.json.gz`), whose EROFS image
was built with `-T0 --mkfs-time`. See the finding below for why the first run's
EROFS numbers are invalid.

| Per sandbox at K=32, after the import | gofer | erofs |
| --- | ---: | ---: |
| cgroup anon | 28.9 MiB | 20.0 MiB |
| Import increment, host Δ AnonPages / K | 15.3 MiB | 12.3 MiB |
| Host Δ Cached / K | 0.4 MiB | 0.4 MiB |
| Dirty file pages | 0 | 0 |
| Sentry RSS → PSS (sum over 32) | 1,613 → 782 MiB | 3,582 → 744 MiB |
| Import p50 / max | 2.93 / 3.17 s | 2.71 / 2.92 s |

**Interpretation.**
- After an import that touches over 100 MiB of files, each sandbox keeps about
  12–15 MiB of new private memory. The rest of its private memory is Sentry
  overhead: about 14 MiB idle cgroup memory for gofer (Sentry USS 8.5 MiB).
- Sentry-native EROFS saves about 9 MiB per sandbox (about 30% of private
  memory) and about 7% of import time. Its Sentry maps the shared image, so RSS
  is large but PSS is small.
- This does **not** support the plan's hypothesis that per-sandbox file caches
  multiply the image's hot set. The memory lever is anonymous and
  Sentry-private memory (the pause tier, C1.1), not image pages (C2.4).

### Finding: `mkfs.erofs -T0` invalidates every Python bytecode cache

The builder runs `mkfs.erofs -T 0` (`environment_builder.py:458`). With
erofs-utils' default `--all-time`, that sets **every file's mtime to 0**.
Python validates timestamp-based `.pyc` files against the source mtime, so
every module imported from such an image is recompiled, and its new `.pyc` is
written into the sandbox's writable layer.

Measured on the same host with no gVisor (chroot over host EROFS plus overlay):

| Image | First import | `.pyc` written to upper |
| --- | ---: | ---: |
| `-T0` (production flags) | 3.74 s | 943 files, 26.2 MB |
| `-T0` again, `.pyc` now in the upper | 0.86 s | — |
| Source directory (real mtimes) | 0.92 s | — |
| `-T0 --mkfs-time -zlz4` (fresh image, cold page cache) | 1.42 s | 0 |

In the first S2 run, the same effect made Sentry-native EROFS imports take
6.6 s, against 3.0 s for gofer, and left 27 MiB of dirty filestore pages per
sandbox. **Every production sandbox on a Python-heavy EROFS image pays this on
first import**: CPU, latency, and bytes in its writable layer that a park must
capture.

`-T0 --mkfs-time` applies the fixed timestamp only as build time and keeps
per-file mtimes. Two builds of the same tree were byte-identical
(sha256 `0e07a2d6…` uncompressed, `aa64d473…` with lz4). Rebuild determinism is
therefore kept for identical source trees.

## Files

- `report-quick.json.gz`: S1, S4, S7 and S8.
- `report-s2.json.gz`: first S2 run, with the `-T0` image. Its `erofs` numbers
  are invalid because of the finding above. Its `gofer` and `gofer-shared`
  numbers are valid; `gofer-shared` also needs `--overlay2=none`.
- `report-s2-mtime.json.gz`: S2 rerun with the `--mkfs-time` image.
- `report-s6-random.json.gz`, `report-s6-compressible.json.gz`: S6 freeze,
  reclaim and fault-back tests.
- `s5-restore-netns.sh`, `s5-fork-two-children.sh`: the S5 scripts. Run them
  only on a disposable host.

Every probe except S5 was produced by `runtime/gvisor/spike_rl_scale.py`.
S3 (multi-lower EROFS in the Sentry) was not run, because S2 no longer
justifies C2.4.
