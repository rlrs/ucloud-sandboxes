# S3 chunk store spike (S12, plan item C2.13), 2026-10-02

The question: **is Hetzner Object Storage (S3) fast enough to serve chunks directly to workers
during a 500-sandbox cold burst?** If yes, M1 uses S3 directly (design Phase A). If not, M1 puts a
store node in front of it (Phase B). Design context: [`chunk-store-design.md`](../../chunk-store-design.md)
§2, §4, §5, §9 and Decisions 1 and 7. Earlier spikes: [S10](../nydus-spike-2026-10-02/README.md) and
[S11](../fscache-spike-2026-10-02/README.md).

## Answer

**No. M1 should start with Phase B's store node in front of S3.** S3 stays the durable store for
packs, bootstraps and chunk maps, as Decision 1 intends.

- **Typical latency is fine; the tail is not.** Ranged GETs had a median of 40–90 ms, the 30–100 ms
  the design assumed. But at every size and concurrency, p95 was 0.5–15 s and p99 3.7–25 s, with
  single requests up to 30 s. That held even one request at a time.
  - 1 MiB GET p99 was **5.5 s at concurrency 32**, the per-worker concurrency a 500-sandbox burst
    implies. The gate is 150 ms.
  - At other concurrencies it was 7–14 s.
- **Workloads inherit the tail.** Cold first commands read through S3 usually matched loopback.
  With trace replay, a cold `import sys` took 0.37–0.39 s against 0.41–0.43 s in S10. But when a
  stalled GET hit, the same command took 13 s, and `pip --version` took 20 s instead of 1.8 s.
  - 4 of the 9 trace-replayed cycles that were captured missed the 1.3× gate.
  - Trace replay removed demand GETs (none in 8 of the 9 cycles). The command still waited, because
    its reads joined the stalled prefetch range.
- **Throughput is limited by the tail.** One client reached at most 131 MiB/s (1 MiB × 128) and
  90 MiB/s at 4 MiB, far from the ≈1,000 GET/s that §2 assumed. 503 SlowDown appeared only at
  64 KiB × 256 (30 of 6,714 requests). The request-rate ceiling we hit was the latency tail, not a
  throttle.
- **What would change the answer:** a repeat on other days showing p99 ≤ 150 ms. One afternoon is
  one sample, but the gap is about 37×. A hedged second GET after 150 ms cut the worst case from 20 s
  to 5.6–7.4 s, which is still about 4× too slow.

**Pass criteria (design §9):**

| Criterion | Result | Verdict |
| --- | --- | --- |
| Trace-replayed cold commands within 1.3× of S10's loopback numbers | 5 of 9 cycles were within 0.90–1.13×. The other 4 were 1.39×, 2.9×, 11× and 29×, all from S3 stalls | **Fail** |
| 1 MiB GET p99 ≤ 150 ms at the burst's concurrency (32 per worker, ≈96 per bucket; see [Derivation](#derivation-500-sandbox-burst-concurrency)) | p99 5,495 ms at 32 from one client. The 3-client run was not measured | **Fail** |
| Index ≤ 45 GB, 30k-id batch lookup ≤ 1 s | 31.3 GB for 500M rows. Lookup took 0.51 s with the file in page cache, 10.5–13.5 s cold, and 3.5 s cold with 4 connections. Inserts ran at 4.5k rows/s against the 10k/s target | **Size passes. Lookup and inserts pass only with a resident index** |

**Two findings that change the design regardless of the answer:**

1. **Production workers would put the gateway back in the byte path.** Workers are private-only
   (`enable_ipv4: false`, `enable_private_egress`), so their Internet egress is NAT through the
   gateway ([`hetzner.md`](../../hetzner.md), "Workers default to private-only networking"). The
   bucket hostname resolves only to a public address (37.27.175.128), and no private endpoint was
   found. So §2's "gateway in the byte path: no" for S3 holds only if workers get public IPv4. A
   store node with public egress on the private network avoids both.
2. **The index needs RAM at full scale.** At 500M rows the 31 GB index meets the 1 s lookup only
   when resident. Cold, each lookup costs about one random 0.3 ms disk read. The gateway (ccx23) cannot hold it
   in memory, though the migrated corpus (≈15M rows, ≈0.95 GB) fits easily.

## What ran, and what was lost

One CCX63 (`sandboxes-spike-s12`, snapshot 438728121, 10.42.0.44 plus a public IPv4) ran from
14:56:31Z to 17:00:21Z, so **2 h 04 min (2.07 VM-hours)**. VM init never ran.

| Part | Status |
| --- | --- |
| (a) corpus and packs | Done: 181 images converted, packed and uploaded |
| (b) S3, 1 client | Done: full matrix, plus latency probes |
| (b) S3, 3 clients | **Not measured.** Both cpx42 clients were refused with `403 resource_limit_exceeded` (`server_shared_cores_limit`, at 15:13Z and 15:35Z). A 3-process run on the CCX63 was queued, but its results were lost (below) |
| (c) sequential cold commands | 204 of 210 cycles ran. 46 of them (images 0, 60 and 63, all modes; image 72, local) were printed to this session and are reported. The rest were lost |
| (c) 20- and 100-way bursts | **Not measured** (queued, results lost) |
| (d) index | Done |
| (e) page-cache sharing, variants i and ii | **Not measured.** Images were selected, converted and packed, and the harness is ready |
| (e) variant iii, EROFS `inode_share` | Steps 1–2 done (module built, fingerprint mechanism found). Step 3 **not run** |

**Why results were lost.** Gateway access in this session depended on an SSH credential that was
not in the checkout: `.hetzner/ssh/gateway-init` was missing, and the first `gw` call failed with
`Permission denied`. From 14:55Z `gw` worked. At about 16:35Z the gateway stopped answering (no SSH
banner, no ping). When it came back at 16:43Z, public-key authentication failed again, through 6 tries
over 15 minutes. The VM's raw files could not be retrieved, so the VM was deleted through the Hetzner
API as the rules require. The numbers below are the ones printed to the session, transcribed into
[`raw/`](raw/). The per-request latency lists and per-cycle backend counters are gone.

**Cleanup.**
- The VM is deleted: `GET /servers/168399354` returns 404, and no server named
  `sandboxes-spike-s12{,-b,-c}` exists.
- **`spike-s12/` is deleted: 985 objects (17.53 GB).** One multi-object delete removed 984 (its
  response was truncated) and one single DELETE removed the last. The bucket's only top-level
  prefix is `prod/` again.
- The S3 credentials were only ever in root-only files: `/root/s12/.s3env` (0600) on the VM, and
  briefly in the gateway's root-only staging directory, deleted at 15:00Z. The VM copy could not be
  removed before deletion, because access had gone. It was destroyed with the VM's disk.
- **Left behind:** the gateway's `/root/s12-staging/` (spike scripts, no credentials) and a
  `known_hosts` entry for 10.42.0.44. Remove both on the next gateway login.
- The 24 h presigned URLs on the VM now point at deleted objects.

## Method

- **Tools:**
  - Nydus v2.4.5 static release from GitHub (sha256 `1ad7b793…19072`, the same as S10);
  - erofs-utils 1.9 and the 7.0.0-30 kernel source and headers from the Ubuntu archive;
  - no Docker Hub pulls.
- **Production access was read-only:** manifests and blobs from `10.42.0.2:5000`. S3 writes went
  only under `spike-s12/`.
- **S3 target:**
  - the live `deployment.json` has empty `registry_store`/`snapshot_store` S3 fields (the stores are
    `filesystem`/`registry`);
  - so the bucket is the design's `ucloud-sandboxes-prod-20260926` at
    `https://hel1.your-objectstorage.com`, region `hel1`, virtual-hosted style
    ([`scripts/make_config.py`](../../../scripts/hetzner_prod/make_config.py)). It holds `prod/`;
  - the VM used its public IPv4, as the brief asked ("as workers would"). Production workers would
    go through the gateway's NAT instead (finding 1).
- **(a) Packs** ([`scripts/packer.py`](scripts/packer.py), [`scripts/packfmt.py`](scripts/packfmt.py)).
  S10's `fetch.py` and `convert.py` (unchanged) fetched the 181-image sample and converted each
  unique layer: `--chunk-size 0x40000 --digester sha256 --compressor zstd --repeatable`, with no
  dictionary, 40 in parallel. Each image was then merged with `--original-blob-ids`. The packer then
  walks S10's shuffled image order (seed 13). For each conversion:
  - every chunk id not yet indexed is read from the local Nydus blob, decompressed and verified
    (`len == ulen`, `sha256 == id`);
  - the chunk is kept as nydus-image's zstd bytes, or raw when zstd saves under 3% or the chunk is
    under 4 KiB;
  - chunks are appended in blob order to packs of at most 64 MiB, in the §1.2 layout (16 B header,
    49 B footer entries sorted by id, 48 B trailer);
  - packs are uploaded to `spike-s12/packs/<hh>/<sha256>.pack`.

  Each image's chunk map and locator (57 B per entry, sorted by device offset), and its zstd
  bootstrap, went to `spike-s12/meta/`.
- **(b) GETs** ([`scripts/s3bench.py`](scripts/s3bench.py), [`scripts/s3lib.py`](scripts/s3lib.py)):
  - SigV4 is stdlib-only and checked against four AWS reference vectors;
  - requests use presigned 24 h URLs, as workers would (Decision 2); the backend holds no key;
  - each request is a `Range` read at a random 4 KiB-aligned offset in a random pack of 8–64 MiB
    (623 packs, 16.9 GB);
  - connections are keep-alive TLS, one per thread, with up to 16 threads per process;
  - cells are 15 s (the first second discarded) with a 30 s read timeout;
  - TTFB runs from send to the status line; total runs to the last body byte.

  Req/s and MiB/s count the whole cell, including overrun by stalled requests, so they are lower
  bounds. [`scripts/probe_conn.py`](scripts/probe_conn.py) and
  [`scripts/probe_cache.py`](scripts/probe_cache.py) separate connection reuse from server-side
  caching.
- **(c) Cold workloads** ([`scripts/s3nbd.py`](scripts/s3nbd.py), [`scripts/coldrun.py`](scripts/coldrun.py)).
  This extends S10's NBD harness.
  - **Backend:** one process serves every device, with one chunk cache keyed by chunk id (memory,
    then disk, verified again on every disk hit) and single-flight fetches. The fetch pool has
    `concurrent_misses` 32, of which at most 8 are prefetch slots (§4).
  - **Attach:** fetch and verify the zstd bootstrap and chunk map from S3, bind NBD, mount EROFS,
    and add an OverlayFS upper. The command runs under `runsc --platform=systrap --network=none run`,
    cold and then warm, as in S10's `mounttest.py`.
  - **Modes:**
    - `local:demand` reads the same packs from local disk: the loopback baseline on the same VM;
    - `s3:demand` merges misses per pack across gaps under 64 KiB, within 1 MiB;
    - `s3:readaround` also extends each window with uncached chunks of this image's map in the same
      pack, up to 1 MiB;
    - `s3:trace` replays the chunk ids that the `s3:readaround` cycle of the same image and command
      touched, as pack-coalesced ranges of at most 8 MiB, at attach;
    - `s3h150:trace` adds a hedged duplicate GET after 150 ms.
  - **Each cycle** is a new backend, an empty cache and `drop_caches`.
  - **Commands:** S10's `import sys` and `git status` (in `/testbed`), plus `python3 -m pip --version`.
  - **Images:** S10's 7.
- **(d) Index** ([`scripts/indexbench.py`](scripts/indexbench.py)).
  - **Table:** the §1.3 `chunks` table (`WITHOUT ROWID`, 32 B id key), on the CCX63's local disk.
  - **Build:** 500M uniform ids, inserted in key order.
  - **Fill factor:** a 20M-row build in both key and random order.
  - **Lookups:** 30k-id batches, half present and half absent. Three query shapes, cold (after
    `drop_caches`) and warm. Then sorted point lookups on 1–64 parallel connections, cold and with
    the file fully in page cache.
  - **Inserts:** 1M new random ids in 7.5k-row transactions (one pack commit each), with WAL,
    `synchronous=NORMAL` and a 4 GB cache.
- **(e) Page cache** ([`scripts/select_pc.py`](scripts/select_pc.py), [`scripts/pagecache.py`](scripts/pagecache.py),
  prepared but not run).
  - **Candidates:** all 3,546 distinct TMax, Terminal-Lego and OpenSWE foundation references in the
    training selection ([`scripts/make_pc_candidates.py`](scripts/make_pc_candidates.py)).
  - **Why bases, not foundation keys:** a `foundation_key` maps to exactly one prepared image, so
    "distinct images on few foundations" here means distinct prepared images that share base layers.
    The images were grouped by their first layer, and 16 were drawn from each of the 4 largest groups.
  - **Result:** 64 images (11 TMax, 53 Terminal-Lego) with 147 unique layers, 6.3 GB unique against
    10.4 GB summed ([`raw/pc-selection.json`](raw/pc-selection.json)).
  - **Harness:** it runs one import-heavy command per image, one image after another. After each, it
    records `/proc/meminfo` (MemAvailable, Cached, Buffers, SReclaimable) and attach time, and it
    reports the slope per extra image. The NBD backend reads local packs with `POSIX_FADV_DONTNEED`
    and caches no chunks, so the growth is the kernel's own.

## Results

### (a) Corpus and packs (181 images, [`raw/packer-main.json`](raw/packer-main.json))

| Measure | Value |
| --- | ---: |
| Conversions (unique non-empty layers) | 618 |
| Packs / bytes | **623 / 16.86 GB** (payload 16.81 GB) |
| Pack size median / p90; packs at the 64 MiB cap | 9.7 MB / 67.1 MB; 182 |
| Chunk references across conversions → unique chunks | 3,499,413 → 874,989 |
| **Duplicate ratio** (already-stored chunks) | **75.0% by count, 70.6% by compressed bytes** (3.40× dedupe over per-layer conversion) |
| Chunks stored raw (under 4 KiB, or zstd saves under 3%); all-zero chunks | 463,020 (53%); 10 |
| Bootstraps raw / zstd; chunk maps + locators zstd | 1.25 / 0.44 GB; 0.23 GB |
| **Total in S3** (packs + zstd bootstraps + maps) | **17.53 GB**, the same as S10's 17.49 GB within 0.2% (M1 gate: within 5%) |
| Pack wall time (single thread: read, verify, write and 16 parallel PUTs) | 166 s for 16.8 GB |

**Per-image pack fan-out** is the number of distinct packs an image's chunk map references:

| Family | n | Median | Max |
| --- | ---: | ---: | ---: |
| ScaleSWE | 60 | 44 | 74 |
| SWE-smith | 10 | 83 | 117 |
| R2E-Gym | 8 | 91 | 96 |
| SWE-Lego | 7 | 79 | 129 |
| SWE-rebench v2 | 5 | 87 | 144 |
| TMax | 40 | 29 | 55 |
| Terminal-Lego | 40 | 23 | 45 |
| OpenSWE foundations | 11 | 39 | 47 |
| **All** | 181 | **39** (p90 79, mean 43.7) | 144 |

- **Bytes are concentrated.** The median image has 90% of its bytes in **9 packs** (p90 18).
- **A cold start touches fewer.** Trace replay coalesced a cold `import sys` into 21–27 ranges and
  `pip --version` into 36–45, so a cold start touches at most that many packs.
- **The page-cache set** (64 images packed into the same index) has a median fan-out of 31.

### (b) S3 ranged GETs, one client ([`raw/s3bench-1client.json`](raw/s3bench-1client.json))

Latencies are in ms. "Errors" are 30 s client read timeouts.

| Size | Conc | Requests | Req/s | MiB/s | Total p50 / p95 / p99 / max | 503 | Errors |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 64 KiB | 1 | 55 | 3.9 | 0.2 | 68 / 462 / 6,871 / 6,871 | 0 | 0 |
| 64 KiB | 8 | 478 | 30.7 | 1.9 | 91 / 792 / 3,890 / 5,790 | 0 | 0 |
| 64 KiB | 32 | 1,675 | 45.3 | 2.8 | 88 / 1,069 / 5,369 / 28,238 | 0 | 0 |
| 64 KiB | 64 | 3,899 | 56.7 | 3.5 | 69 / 841 / 3,676 / 17,480 | 0 | 5 |
| 64 KiB | 128 | 5,032 | 68.3 | 4.3 | 46 / 2,449 / 8,708 / 20,387 | 0 | 12 |
| 64 KiB | 256 | 6,714 | 90.8 | 5.7 | 39 / 3,254 / 15,737 / 29,932 | **30** | 57 |
| **1 MiB** | 1 | 25 | 1.5 | 1.5 | 70 / 2,421 / 11,920 / 11,920 | 0 | 0 |
| **1 MiB** | 8 | 343 | 5.2 | 5.2 | 79 / 876 / 6,978 / 18,889 | 0 | 1 |
| **1 MiB** | **32** | 1,928 | 48.6 | 48.6 | 60 / 808 / **5,495** / 12,758 | 0 | 0 |
| **1 MiB** | 64 | 1,549 | 25.0 | 25.0 | 70 / 4,366 / 13,957 / 28,705 | 0 | 1 |
| **1 MiB** | 128 | 3,738 | 130.9 | 130.9 | 63 / 3,234 / 13,033 / 28,600 | 0 | 0 |
| **1 MiB** | 256 | 5,863 | 82.1 | 82.1 | 288 / 3,146 / 9,576 / 28,071 | 0 | 4 |
| 4 MiB | 1 | 19 | 1.1 | 4.6 | 157 / 5,839 / 5,839 / 5,839 | 0 | 0 |
| 4 MiB | 8 | 189 | 2.6 | 10.5 | 140 / 2,794 / 25,363 / 28,967 | 0 | 1 |
| 4 MiB | 32 | 500 | 7.2 | 28.9 | 150 / 7,454 / 19,269 / 28,152 | 0 | 2 |
| 4 MiB | 64 | 397 | 4.6 | 18.5 | 223 / 14,582 / 24,452 / 28,369 | 0 | 1 |
| 4 MiB | 128 | 1,648 | 22.5 | 89.8 | 143 / 9,525 / 23,660 / 29,718 | 0 | 33 |
| 4 MiB | 256 | 1,388 | 19.6 | 78.3 | 2,675 / 7,503 / 23,021 / 28,868 | 0 | 4 |

- **TTFB carries almost all the latency.** For 1 MiB at 64, TTFB p50/p95/p99 were 58/2,848/13,314
  ms, against totals of 70/4,366/13,957. Stalls happen before the first byte as well as mid-stream.
- **A first attempt at 15:14Z with 20 s cells showed the same shape.** 64 KiB × 32: p50 51 ms, p95
  5.1 s, p99 21.8 s, with 11 timeouts.
- **Client CPU was not the limit:** at most 48 CPU-s over a 15 s cell on 48 vCPUs.

**Probes** ([`raw/s3-probes.json`](raw/s3-probes.json)):
- **Sequential reads still stall.** 40 sequential 64 KiB GETs: fresh connections p50 47–84 ms,
  p90 0.3–4.3 s, max 3.4–23 s; keep-alive p50 49 ms, p90 152 ms, max 5.0 s, with 2 timeouts.
  Connection reuse is not the cause.
- **The server caches recently read ranges, but cached reads still stall.** On the same 30 random
  ranges:
  - cold: p50 59 ms, max 22 s;
  - re-read: p50 **6 ms**, still max 11 s;
  - adjacent ranges: like cold.
  - curl re-reading one range: TTFB 15–41 ms, including a new TLS handshake.
- **Whole 64 MiB packs:** 152 MB/s, 107 MB/s, and one at **4 MB/s (16.1 s)**, so bulk reads stall too.
- **Private endpoint:** none found. The hostname resolves only to the public 37.27.175.128.

**Three clients.** Not measured (see [What ran](#what-ran-and-what-was-lost)). One client already
fails at every concurrency, and adding clients cannot lower a tail that occurs even at
concurrency 1.

#### Derivation: 500-sandbox burst concurrency

- **The burst:** C2.6's budget case is 64 tasks × 8 rollouts, 512 sandboxes on 3 CCX63 workers
  (§2).
- **Per worker:** every worker's single storage backend bounds in-flight fetches with its miss pool
  (`VerifiedEnvironmentCache(concurrent_misses=…)`, which §4 step 4 raises from 8 to 32; prefetch
  takes at most a quarter). A burst saturates the pool, so each worker issues about **32 concurrent
  GETs**.
- **At the bucket:** 3 × 32 ≈ **96**. The gating cell is therefore 1 MiB at 32 per client with 3
  clients.
- **Results:** the 1-client cell already fails, at p99 5.5 s. The 64 and 128 cells, which stand for
  a bigger pool or more workers per bucket, fail at 13–14 s.
- **Demand rate:** at the design's 1 GB/s burst target in 1 MiB windows, about 1,000 GET/s. The
  best observed was 131 GET/s from one client.

### (c) Cold first commands through S3 ([`raw/coldseq-captured.json`](raw/coldseq-captured.json))

Cold wall time in seconds. S10's loopback numbers are in brackets for `import sys` and `git status`.
For `pip --version`, `local:demand` on this VM is the baseline; S10 did not run `pip`.

| Image | Mode | `import sys` | `git status` | `pip --version` | GETs, demand + prefetch (import / git / pip) | MB fetched / MB touched (import / git / pip) |
| --- | --- | ---: | ---: | ---: | --- | --- |
| 0 ScaleSWE (S10: 0.46 / 0.25) | local:demand | 0.49 | 0.28 | 1.79 | 59 / 15 / 520 | 3.8/3.8 · 1.1/1.1 · 12.3/12.3 |
| | s3:demand | 1.50 | 0.37 | **24.89** | 59 / 15 / 519 | same as local |
| | s3:readaround | 1.17 | 0.38 | 13.36 | 22 / 5 / 46 | 21.7/3.8 · 5.1/1.1 · 44.7/12.3 |
| | **s3:trace** | **13.07** | 0.25 | **19.95** | 0+21 / 1+6 / 0+36 | 4.8/3.8 · 2.1/1.1 · 15.4/12.1 |
| | s3h150:trace | – | 0.26 | 7.38 | – / 1+6 / 0+36 | – · 2.1/1.1 · 15.4/12.3 |
| 60 SWE-Lego (S10: 0.43 / 0.31) | local:demand | 0.45 | 0.40 | 1.79 | 55 / 85 / 509 | 4.0/4.0 · 4.8/4.8 · 12.6/12.6 |
| | s3:demand | 12.52 | 2.92 | 15.07 | 54 / 84 / 507 | same as local |
| | s3:readaround | 4.78 | 1.19 | 3.63 | 26 / 19 / 54 | 23.1/4.0 · 16.0/4.8 · 48.7/12.6 |
| | **s3:trace** | **0.39** | **0.32** | **2.02** | 0+27 / 0+16 / 0+43 | 4.5/4.0 · 5.0/4.8 · 18.4/12.6 |
| | s3h150:trace | 0.38 | 0.33 | 7.23 | 0+27 / 0+16 / 0+43 | as trace |
| 63 SWE-smith (S10: 0.41 / 0.50) | local:demand | 0.47 | 0.66 | 1.87 | 58 / 270 / 523 | 4.9/4.9 · 5.4/5.4 · 13.7/13.7 |
| | s3:demand | 16.50 | 27.17 | 36.63 | 59 / 270 / 522 | same as local |
| | s3:readaround | 14.17 | 1.58 | 15.82 | 27 / 23 / 56 | 24.4/4.9 · 17.6/5.4 · 49.7/13.7 |
| | **s3:trace** | **0.37** | **1.44** | **2.60** | 0+27 / 0+21 / 0+45 | 5.0/4.9 · 6.4/5.4 · 18.9/13.7 |
| | s3h150:trace | 0.98 | 0.53 | 5.63 | 0+27 / 0+21 / 0+45 | as trace |
| 72 R2E-Gym (S10: 0.50 / 0.52) | local:demand | 0.55 | – | – | 78 / – / – | 7.3/7.3 |

**Ratios for trace replay, against S10** (`import sys`, `git status`) **and against local**
(`pip --version`):

| Image | `import sys` | `git status` | `pip --version` |
| --- | --- | --- | --- |
| 0 | **28.7×** | 1.01× | **11.1×** |
| 60 | 0.90× | 1.03× | 1.13× |
| 63 | 0.91× | **2.89×** | **1.39×** |

- **Harness parity.** The local baseline is within 1.05–1.15× of S10 for `import sys`
  (0.45–0.55 s against 0.41–0.50 s), so the harness matches S10's.
- **Requests.** Demand-only issued 54–59 GETs for `import sys` and 507–523 for `pip`, close to one
  per chunk run. 1 MiB read-around cut that to 22–27 and 46–56 (2–11× fewer). Trace replay needed
  no demand GETs in 8 of 9 cycles.
- **Amplification** (bytes fetched over compressed bytes touched):
  - read-around: **3.2–5.8×**;
  - trace replay: 1.03–1.45×;
  - demand-only: 1.0×.
- **Every slow cycle is an S3 stall,** whatever the mode. Local cycles never exceeded 1.9 s.
  Demand-only `git status` on image 63 took 27 s for 270 GETs. Read-around hides fewer stalls than
  trace replay but still hit them: 13–16 s.
- **Hedging at 150 ms** (a duplicate GET on another connection, first reply wins) bounded `pip` at
  5.6–7.4 s, against up to 20 s without it. But it was slower than plain trace replay when S3
  behaved (2.0–2.6 s), because most 8 MiB prefetch ranges legitimately take over 150 ms and were
  duplicated. A size-aware threshold is needed, and even then the tail stays in seconds.
- **Attach.** Fetching the bootstrap and map from S3 took 0.17–0.39 s, except one 2.66 s attach.

The 20- and 100-way bursts were not measured.

### (d) Index at 500M rows ([`raw/index.json`](raw/index.json))

| Measure | Value |
| --- | ---: |
| File size, key-order build | **31.34 GB** (62.7 B/row; the design estimated 80 B and 40 GB) |
| Random-order insertion against key order (20M-row build) | **0.98×**, so random insertion is no larger |
| Build, key order with journal off | 1,120 s (446k rows/s) |
| 30k-id batch, cold, one connection (`IN` × 999 / point / temp-table join) | 12.5 / 11.8 / 10.5 s |
| 30k-id batch, further batches without `drop_caches` | 5.9–10.0 s |
| 30k-id sorted point lookups, cold, 1 / 4 / 16 / 64 connections | 13.5 / **3.5** / 4.2 / 5.2 s |
| 30k-id sorted point lookups, file fully in page cache, 1 / 4 / 16 / 64 connections | **0.51** / 0.54 / 4.8 / 5.1 s |
| Inserts: 1M random ids in 7.5k-row transactions (WAL, `synchronous=NORMAL`) | **4,513 rows/s** |

- **Cold lookups are disk-bound.** Each random id costs about one leaf read, about 0.3 ms at
  queue depth 1 on this virtual disk.
- **Parallelism helps only up to 4 connections.** Beyond 4, the shared-memory and WAL locks of the
  Python `sqlite3` connections dominate, even fully cached.
- **The 1 s gate holds only for a resident index.**
- **Inserts** meet §5's need of 3.4k/s but not S12's 10k/s target. They were measured on a mostly
  cold cache, so a resident index would do better (not measured).

### (e) Page-cache sharing (Decision 7)

The measurement was not run, so the comparison table has no numbers yet:

| Variant | Memory per extra distinct image | Attach |
| --- | --- | --- |
| (i) per-image RAFS mounts (one merged bootstrap and one NBD device per image) | not measured | not measured |
| (ii) per-layer RAFS mounts stacked with OverlayFS, identical layers shared | not measured | not measured |
| (iii) per-image mounts with EROFS `inode_share` in one `domain_id` | not measured | not measured |

The 64-image set, its packs, the chunk maps of every layer and image, and the harness are ready
(`pagecache.py prep` builds the fingerprinted images; then `run --variant image|layer|ishare|erofs`,
where `erofs` is the same images without `inode_share`, the control for iii).

**Variant iii: what was established** ([`raw/erofs-ishare.json`](raw/erofs-ishare.json)):

1. **A module rebuild is enough.** The kernel has `CONFIG_FS_STACK=y` (built in) and
   `CONFIG_EROFS_FS_XATTR=y`. The option is `EROFS_FS_PAGE_CACHE_SHARE`, "EROFS page cache share
   support (experimental)", which depends on `EROFS_FS_XATTR && !EROFS_FS_ONDEMAND`.
   - S11's out-of-tree recipe, with `CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y
     KCFLAGS=-DCONFIG_EROFS_FS_PAGE_CACHE_SHARE=1`, built `erofs.ko` with vermagic
     `7.0.0-30-generic SMP preempt mod_unload modversions` and 26 `ishare` symbols (sha256
     `2dde3450…babd7b6`, [`scripts/build_erofs_ishare.sh`](scripts/build_erofs_ishare.sh)).
   - It was not loaded, because (e) never started.
   - Like S11's modules, it would be unsigned and out-of-tree. Unlike fscache, the feature is
     experimental rather than deprecated.
2. **How a file gets its fingerprint** (`fs/erofs/ishare.c`, `xattr.c`, `super.c`):
   - **Superblock:** compat bit `0x20` (`COMPAT_ISHARE_XATTRS`), plus `ishare_xattr_prefix_id`, the
     u8 at superblock offset 105. That byte indexes the long xattr name prefix table
     (`INCOMPAT_XATTR_PREFIXES`, `0x40`).
   - **Per file:** each regular file's fingerprint is the value of the xattr named exactly by that
     prefix. The value is opaque bytes, at most one block.
   - **Kernel side:** the kernel appends the mount's `domain_id`, hashes the result with xxh32, and
     shares one inode's page cache (`iget5_locked`) between files whose full fingerprints match. It
     refuses to share when the sizes differ.
   - **Mount:** `-o inode_share,domain_id=<id>`. Without the on-disk feature, the kernel logs
     "on-disk ishare xattrs not found. Turning off inode_share."
3. **Which tools can produce it:**
   - **nydus-image v2.4.5 cannot.** It has no such option; `--digester` is for chunk digests.
   - **mkfs.erofs 1.9 can, natively:** `--xattr-inode-digest=<name>`. A test image got compat
     `0x37` (which includes `0x20`), incompat `0x40`, one long prefix and `ishare_xattr_prefix_id`
     0. It also takes `--tar=f|i` and `--oci=…[,insecure]` input.
   - **Post-processing a RAFS bootstrap** would mean adding the prefix table and an xattr to every
     regular file's inode body. That changes inode sizes, so it is a metadata relayout, not a patch.
   - **Least custom code:** build fingerprinted metadata with mkfs.erofs 1.9, or have our own
     converter emit the xattr. Either moves us off nydus-image's bootstrap.

## Recommendations for M1

1. **Use Phase B from the start: a store node in front of S3.**
   - Use §2's `ucloud-store` (nginx `slice 1m`, `proxy_cache` on NVMe, signing its own upstream
     requests). Put workers' demand reads on the private network to the store, and keep S3 as the
     durable store and fill source.
   - Writes were fine: 623 packs (16.9 GB) uploaded during a 166 s packing run, with no failed PUT.
   - **Move the §2 trigger:** S12 shows demand-miss p99 far over 150 ms, so C2.6 precedes W9.
2. **Fill and timeouts belong on the store node, not the worker.** Use bounded upstream timeouts
   with retry or hedging (a second GET after 2× the size-specific p95) on the store tier, plus
   pack-level prefetch: hydrating an image's packs (median 39, 90% of bytes in 9) as whole-object
   GETs streams at 100–150 MB/s. The stall rate still makes any S3 read a possible multi-second
   wait, so nothing a sandbox waits on should go to S3 directly.
3. **Give store nodes public egress, or accept gateway NAT for fills only.** Workers stay
   private-only, and the gateway is never in the demand path.
4. **Window sizes** (for the store-node path, with an assumed 1–3 ms per request):
   - Keep **demand windows at 1 MiB with read-around**. It cut GETs 2–11× at 3–6× byte
     amplification, which is cheap from a local store, and it matches nginx's `slice 1m`.
   - Keep **trace replay as pack-coalesced ranges of at most 8 MiB**. It removed demand misses (0 in
     8 of 9 cycles) at 1.03–1.45× amplification.
   - `concurrent_misses` 32 is not needed for a store at 1–3 ms. Keep 8–16 per worker, and size the
     store tier for the burst's aggregate instead.
5. **Run the index service against a resident or parallel index.** 15M rows (≈0.95 GB) fit the
   gateway's RAM. At full corpus, host it with ≥ 48 GB RAM (store node 1, as §2 Phase B already
   plans) or issue lookups on about 4 connections. Recheck inserts at 10k/s on a resident index in M1.
6. **Mount granularity: keep per-image RAFS mounts for M1, as designed, and run (e) early in M1.**
   The harness and image set are ready. If per-image mounts cost too much page cache, the kernel
   route is `inode_share`. It needs a rebuilt erofs.ko and fingerprints that nydus-image cannot emit:
   mkfs.erofs 1.9 `--xattr-inode-digest`, or our converter. Defer that decision until (e) has
   numbers.
7. **Repeat (b) and the bursts on another day before closing the question,** with 3 real client
   VMs. Free the shared-core quota first, or allow a dedicated type for the clients. Treat the
   result as confirmation, not as a reason to delay Phase B: closing the gap needs about 37× better
   p99.

## Files

- **[`raw/`](raw/)**, transcribed from console output:
  - `packer-main.json`, `packer-pc.json`: (a);
  - `s3bench-1client.json`, `s3-probes.json`: (b);
  - `coldseq-captured.json`: (c), with S10's baselines;
  - `index.json`: (d);
  - `pc-selection.json`, `erofs-ishare.json`: (e);
  - `runtime.json`: VM, S3 and cleanup ledger.
- **[`scripts/`](scripts/)**, run on the VM unless marked otherwise. S10's `fetch.py`, `convert.py`
  and `rafs.py` were copied unchanged from [`../nydus-spike-2026-10-02/scripts/`](../nydus-spike-2026-10-02/scripts/),
  and S10's `raw/sample.json` was the sample.
  - **Setup:** `setup.sh`, `run_a.sh`; `kernel_setup.sh` and `build_erofs_ishare.sh` (iii).
  - **(a):** `packer.py`, `packfmt.py`.
  - **(b):** `s3lib.py` (SigV4, presign, prefix-guarded writes), `presign.py`, `s3bench.py`,
    `probe_conn.py`, `probe_cache.py`, `s3.json`; `run_b3.sh` (the 3-process run; queued, results
    lost).
  - **(c):** `s3nbd.py`, `coldrun.py`; `run_bursts.sh` and `burst-images.json` (queued, results
    lost).
  - **(d):** `indexbench.py`.
  - **(e):** `make_pc_candidates.py` (run locally on the training-selection zip), `select_pc.py`,
    `pagecache.py` (prepared, not run).

  These are spike code, not product code.
