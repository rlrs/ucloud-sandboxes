# nydusd as the native image device (C2.1 candidate), 2026-10-03

**Question.** Can stock nydusd replace the Python NBD backend as the worker's
image device (C2.1) without fscache? It would read "virtual blobs" that the
store node assembles from our packs. It must verify every chunk, and it must
fail closed when it dies.

**Answer: yes, and it is the largest burst win so far.**

Five findings:

1. **Speed.** On the same 64 cold images, nydusd's NBD export takes the burst
   from 31.5 s (today's path, demand) to 18.6 s with serial attach. With
   `--attach-concurrency 8` it takes 12.0 s, against 24.1 s for today's path
   with startup traces. nydusd needs no traces. Median `import sys` falls from
   0.98 s to 0.18 s, and median `pip --version` from 3.8 s to 1.0 s.
2. **Fails closed.** A corrupted chunk reads as EIO. `kill -9` of 8 of 64
   daemons mid-read gave EIO in exactly those 8 sandboxes. The other 56 were
   untouched, and the backend refused new creates of the 8 dead images.
3. **Trusted end to end.** It needs conversion with `nydus-image --features
   blob-toc`. The signed bootstrap then pins a TOC, the TOC pins every chunk
   digest, and nydusd checks each chunk it fetches.
4. **The cost is bytes and processes.** nydusd fetched 4.2× the bytes of our
   Python reader (1.50 GB against 0.36 GB for 64 images). It runs one process
   per image, about 23 MB each.
5. **gVisor's filesystem path is the next lever, not the main one.** Warm
   commands in the sandbox take about 2× native. That is about 0.2 s of a
   1.1 s cold `pip --version`. Most of a cold command is still getting the
   bytes in.

## nydusd's modes in 2026

nydusd master (3.0 betas, more than 3,600 commits past v2.4.5) and v2.4.5 have
the same mode set. Neither has a fanotify or file-backed EROFS service: upstream
still maintains fscache (it carried a July 2026 patch adapting fscache to newer
kernels). The fanotify pre-content successor exists only in erofs-utils, which
S13 used with our own daemon.

| mode | what it serves | fits runsc? |
| --- | --- | --- |
| `fuse` | a FUSE mount; nydus-snapshotter's default | yes, but every uncached lookup and read is a round trip; its one edge is failover (a supervisor keeps `/dev/fuse` and a new daemon takes over) |
| `nbd` | a RAFS v6 image as one NBD block device: the bootstrap, then each blob at its `mapped_blkaddr` | **yes: exactly our device model** (marked "Experiment"; not in release binaries, so built with `--features block-nbd`) |
| `uffd` | the image as an mmap region over a socket, filled through userfaultfd | no: it is for a VMM exposing virtio-pmem DAX to a guest |
| `virtiofs` | a vhost-user filesystem | no: VMs only |
| fscache | EROFS over cachefiles | rejected in S11 (deprecated, a restart breaks mounts) |

## Design

- **Conversion** uses `chunk_convert --nydusd-blobs`, which does three things:
  - runs `nydus-image create --features blob-toc`;
  - keeps nydus's own zstd chunk bytes, even when they save under 3%;
  - stores each blob's tail object, `meta/<blob>.tail`. It holds the blob's
    full chunk table, then the bytes after its last chunk: chunk info,
    digests and the TOC.

  It has its own converter identity, so it never shares layer claims with M1
  conversions. On this sample the tails were 73 MB against 6.54 GB of packs.
- **Store node** (`UCLOUD_CHUNK_STORE_VIRTUAL_BLOBS`). It serves `GET` and
  `HEAD /v2/virtual/<component>/blobs/sha256:<blob>` with the read token,
  which is the shape of nydusd's registry backend.
  - **Rebuilding a blob:** the chunk table from the tail, locations from the
    index (`locate`, with the write token as builders use), then extents and
    sendfile.
  - **Sharing:** the result is the original blob byte for byte, cached per blob
    and shared by every image. It is tested with fake and real nydus-image
    (`tests/test_nydusd_spike.py`).
  - **Why the full table:** a merged image's bootstrap omits chunks that only
    whiteout-hidden files use. Zero-filling them broke reads, because nydusd
    and the kernel's block readahead fetch neighbouring chunks in one request.
- **Worker** (`UCLOUD_ENVIRONMENT_NYDUSD`). Each RAFS image gets one
  `nydusd nbd /dev/nbdN` with:
  - the registry backend pointed at the store node, with the read token as the
    bearer;
  - `filecache` with `validate: true`;
  - our verified bootstrap file as `metadata_path`.

  Attach, the device lease, EROFS mounting and fencing are unchanged. Other
  components keep the Python export.
- **Trust chain:**
  - the signed component pins the bootstrap;
  - the bootstrap pins `blob_toc_digest` (`toc.rs` checks it);
  - the TOC pins the chunk-digest array;
  - `validate` checks every fetched chunk against that array.

  Offsets come from the unpinned chunk info, but a wrong offset yields bytes
  that fail their digest. nydusd does not check `blob_meta_digest`, so without
  `blob-toc` it would trust the store node.

Local proof before any VM:
- the device content was byte-identical to `nydus-image export --block`;
- a flipped byte gave `data digest value doesn't match` and EIO;
- `kill -9` disconnected the device;
- 1,026 of 1,026 chunk regions matched our Python RAFS reader.

## Setup

- **Driver.** The M1 gate driver (`scripts/chunk_store_gate.py`) ran with new
  options, which set the stack up and run no bench:
  `--sample raw/sample.json --burst-images 0..63 --holdout-count 0
  --nydusd-blobs --skip-bench --workers 1 --worker-type ccx63`.
- **Run.** Run `20261003t1247`, S3 prefix `spike/m1/20261003t1247`.
- **Hosts:**
  - store node: CCX43;
  - converter: CCX63;
  - canary `w1`: CCX63, with the bundle repacked from 0.8.4 with this branch's
    wheel (`0.8.5.dev0+nydusd`, bundle `1d928981`);
  - baseline `b1`: CCX63, on 0.8.4 and today's path, reading the production
    registry read-only.

  All came from snapshot `438866767`.
- **Images.** The attach spike's 64 production images. 11 of them have no
  Python, so they fail the bench's commands in every arm, today's included.
- **Conversion.** All 64 converted and verified through a mount in 15 min:
  - 240,430 chunks;
  - 6.54 GB of packs;
  - 167 layers.
- **nydusd.** v2.4.5 built from the tag with `--features block-nbd` (sha256
  `96b8d9a7…`). Its threads were the default 4.
- **Arms.** On `w1`, every arm starts from a cold node: the backend restarted,
  the cache emptied and the page cache dropped. A warm-up run filled the store
  node first, so every arm reads a warm store node.
  - Scripts: [nydusd_spike_run.sh](nydusd_spike_run.sh) and
    [nydusd_spike_worker.py](nydusd_spike_worker.py) (`arm`, `kill`, `split`).
  - `b1` ran [baseline_run.sh](baseline_run.sh).
  - Raw results are in [raw/](raw/).
  - nydusd keeps no traces, so its "traced" row is a second cold run.

## Results

**64 images** (bench burst: create, then `import sys` and `pip --version`):

| path | wall, s | create p50 / max, s | `import sys` p50 / p95, s | `pip` p50 / p95, s | from store |
| --- | ---: | ---: | ---: | ---: | ---: |
| **nydusd, attach 8** | **12.0 / 11.5** | 5.9 / 10.0 | **0.18** | **1.01** | 1.50 GB, 18,977 req |
| nydusd, serial attach | 18.6 / 17.9 | 8.9 / 16.8 | 0.25 / 0.37 | 1.31 / 1.54 | 1.50 GB, 18,978 req |
| Python RAFS, demand | 38.1 | 12.2 / 28.4 | 3.84 / 8.4 | 12.4 / 27.8 | 0.36 GB, 339 req |
| Python RAFS, traced | 26.3 | 11.8 / 24.8 | 0.46 / 0.90 | 2.93 / 4.29 | 0.23 GB, 415 req |
| today (0.8.4), demand | 31.5 | 9.8 / 27.0 | 0.98 / 2.67 | 3.83 / 9.05 | registry |
| today (0.8.4), traced | 24.1 | 7.8 / 22.1 | 0.39 / 1.19 | 2.12 / 6.09 | registry |

Today's numbers repeat the attach spike's 31.2 and 24.6 s.

**20 images:**

| path | wall, s | create p50, s | `import sys` p50, s | `pip` p50, s | from store |
| --- | ---: | ---: | ---: | ---: | ---: |
| nydusd, serial attach | 7.8 / 7.0 | 3.6 | 0.26 | 1.36 | 0.44 GB |
| Python RAFS, demand / traced | 13.1 / 9.2 | 3.6 / 4.0 | 2.81 / 0.59 | 4.97 / 2.41 | 0.17 / 0.09 GB |
| today, demand / traced | 10.2 / 7.3 | 2.3 / 2.5 | 0.78 / 0.33 | 4.73 / 2.50 | registry |

**Host cost, 64 images, serial attach:**

| path | environment-io CPU, s | nydusd share, s | peak daemon RSS |
| --- | ---: | ---: | ---: |
| nydusd | 40 | 26 | 1.49 GB (64 × 23 MB) |
| Python RAFS, demand / traced | 62 / 42 | — | — |

The environment-io figure includes the CPU of reaped nydusd children.

### Reading it

- **The fetch path was the cap, and nydusd removes it.**
  - It issues small requests, about 79 KB each, and the store node serves
    18,977 of them in a burst.
  - The Python reader issued 339 requests at about 1 MiB, then ran out of
    interpreter (the attach spike measured 40–50 MB/s per node).
  - nydusd moved 1.50 GB in 18.6 s, about 80 MB/s, while every first command
    finished faster.
- **Wall time is now the attach staircase, and parallel attach pays.**
  - With serial attach, create max is 16.8 s, the same staircase as before.
  - Attach 8 cuts wall time by 35%, and first commands get faster, not slower.
    This is the opposite of 0.8.3, where parallel attach lost to a saturated
    miss path.
- **Bytes are the price.** nydusd fetches 4.2× what our reader does. Three
  reasons:
  - each process has its own cache, so images do not share layers (our
    reader's cache is per chunk id);
  - nydusd amplifies reads;
  - the kernel reads ahead on the block device (`readaround`).

  On a private network to a warm store node that was cheap here. At fleet
  scale it multiplies store-node egress, so a shared cache work dir or a
  tighter `amplify_io` should come before production.
- **No traces needed.** nydusd's cold runs beat our traced runs. On this path
  trace replay (C2.7) has nothing left to win in this burst; whether traces
  help nydusd's own prefetch was not tested.

## kill -9 mid-read ([raw/kill.json](raw/kill.json))

**Method:**
- 64 images were attached serially, one nydusd each.
- Every sandbox then read up to 20,000 files of `/usr /opt /lib`.
- 3.05 s in, 8 random daemons got `kill -9`.

**Results:**

| | killed daemons' sandboxes (8) | other sandboxes (56) |
| --- | --- | --- |
| the read in flight | `Input/output error`, `xargs` exit 123 | finished, exit 0 |
| a later read of `/usr/bin/*` | EIO again | no EIO |
| a new create of the same image | refused (HTTP 503: the backend sees a dead export and fences) | created and ran |

The set that saw EIO equals the set refused at recreate. No sandbox read
anything but its image's bytes or EIO. As with today's Python export, there is
no recovery for a mount whose daemon died: the sandbox is drained. nydusd's
NBD mode has no takeover; only FUSE and fscache do.

## Where first-command time goes ([raw/split.json](raw/split.json))

Six images with Python. Each command ran cold (the Python RAFS path, with a
warm store node), then three times warm in the same sandbox (timed inside it with `date +%s%N`), then warm natively with
`chroot` into the same host rootfs. `PYTHONDONTWRITEBYTECODE=1` was set for
all runs.

| command | cold, sandbox | warm, sandbox | warm, native | sandbox ÷ native |
| --- | ---: | ---: | ---: | ---: |
| `true` | 0.016 | 0.015 | 0.006 | 2.5× |
| `import sys` | 0.14–0.42 (median 0.24) | 0.06–0.09 | 0.015–0.05 | about 2× |
| `pip --version` | 0.92–1.68 (median 1.08) | 0.40–0.76 | 0.19–0.58 | about 2× |

- **directfs is on.** Our runsc (20260817 plus patches) runs with
  `--gofer-mount-confs=lisafs:self` and no `--directfs` override. Each sentry
  held 29–67 host file fds and 4 sockets. So rootfs lookups and reads are host
  syscalls made by the sentry, not gofer round trips. The `:self` overlay
  keeps the sentry's own upper layer.
- **gVisor costs about 2× native when warm,** which is about 0.2 s of a 1.1 s
  cold `pip --version`. That covers every syscall under systrap, not only the
  filesystem.
- **The cold remainder, about 0.65 s, is getting the bytes in.** That is what
  nydusd shrinks.
- **A sentry-native EROFS mount would attack the 0.2 s.** gVisor has
  `pkg/erofs`, which mmaps an image and resolves lookups in the sentry, but
  it rejects chunk-based inodes and ignores the device table, both of which
  RAFS v6 needs. It is worth a later spike: a bounded gVisor patch, worth at
  most about 20% of a cold command. It is not the next lever.

## Bugs found on the canary (fixed)

- **`dbfbcc8`:** HEAD read the whole blob, so blobs over 256 MiB got 416 and
  nydusd could not size them. HEAD now answers from the layout. The store node
  was hot-patched with exactly this change.
- **`235e452`:** a leased NBD device made `NydusdDevice` raise AttributeError
  instead of EBUSY, so attach stopped probing other devices. The first
  64-image nydusd run failed on this; the numbers above are its rerun.

## What this leaves (before production)

- **Reconversion.** M2 converts with `--nydusd-blobs`, a new converter
  identity. Nothing in production is converted yet, so the cost is the tails
  (about 1% of stored bytes) and a store that never re-encodes chunks.
- **Retention.** A blob's tail table names chunks that no root's chunk map
  holds (whiteout-hidden ones). Retention and compaction must keep them alive
  while the blob is live.
- **Byte amplification:**
  - one shared nydusd cache across images (one daemon per image is the `nbd`
    model; a shared `work_dir` needs testing for cross-process safety);
  - or tune `amplify_io` and the block readahead.
- **Packaging.** Build and pin nydusd with `block-nbd` ourselves, since no
  release has it, and track that the mode is marked experimental upstream.
  Rust, about 23 MB per image daemon.
- **Defaults.** Make attach concurrency (8 here) the default together with
  nydusd, not before.
- **Not measured:** 512-way across nodes, store-node limits under many workers,
  and long-lived daemons (leaks, reconnects to a restarted store node).

## Resources

- Store node CCX43, converter CCX63, `w1` and `b1` CCX63: about 3.1 VM-hours,
  EUR 1.53 at list price billed per started hour.
- Teardown drained the workers, deleted the run's S3 prefix and all four VMs,
  the gate staging directory and their known_hosts entries.
- `/work/ucloud-sandboxes/nydusd-spike-20261003` on the gateway (bundle,
  wheel, nydusd, scripts) remains, like earlier spikes' staging directories.
