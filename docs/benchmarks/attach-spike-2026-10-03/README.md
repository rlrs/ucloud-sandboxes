# Attach cost spike: per-node create rate on 0.8.4 (2026-10-03)

**Question.** The 512-sandbox rollout baseline
([rl-scale-rollout-2026-10-03](../rl-scale-rollout-2026-10-03/README.md))
queued 20–80 s per node after its node came up, though a worker-side create
took 0.38 s. Is that per-node serialization of component attach, and can
attach be made cheaper?

**Answer.**
- Creates are metered through serial attach, but **attach is cheap: about
  170 ms per component.**
- The real per-node limit is **image data from the gateway's registry, about
  45 MB/s.** That comes from a **request-rate ceiling of about 180 requests/s**,
  and the worker requests one 256 KiB chunk per miss.
- At 4 MiB per request the same registry serves **730–750 MB/s** from page
  cache and **about 400 MB/s** cold off the Volume.
- **The fix is coalescing misses into larger reads**, not cheaper or
  parallel attach.

## Setup

- **Worker:** one disposable CCX63 from the production snapshot `438866767`
  with the live production config, private-only at 10.42.0.60. Its bundle was
  repacked from 0.8.4, swapping only the agent wheel: HEAD `3ace34b`,
  `0.8.5.dev0+attachspike`, for the opt-in attach timing log
  (`UCLOUD_ENVIRONMENT_TIMING_LOG`).
- **Images:** 64 distinct production images (29 ScaleSWE, 26 Terminal-Lego,
  6 TMax, 2 R2E-Gym, 1 SWE-smith). They have 136 components and 35 GB of EROFS.
  The first 20 form the small set.
- **Burst:** the M1 gate's bench (`chunk_store_gate_remote.py bench --kind
  burst`) sends all creates at once to the node API, so no gateway is
  involved. Each burst runs `import sys` and `pip --version`. Every mode
  starts cold: fresh backend, empty chunk cache, dropped page cache. The
  demand mode also clears traces; the traced mode replays them.
- **Scripts:** the runs are [attach_spike_run.sh](attach_spike_run.sh) and
  [attach_spike_misses.sh](attach_spike_misses.sh). The second patches only
  the spike VM, so that environment-io takes its concurrent-miss limit from
  an environment variable. The raw read probe is
  [registry_probe.py](registry_probe.py). Results are in [raw/](raw/).

## Results

**Bursts of 64 images** (wall time in seconds, create p50 / max in seconds,
first-command p50 in seconds):

| attach slots | misses in flight | mode | wall | create | `import sys` | `pip --version` | fetched, MB/s |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 8 | demand | 31.2 | 8.0 / 26.4 | 1.05 | 3.6 | 41 |
| 1 | 8 | traced | 24.6 | 8.5 / 22.5 | 0.43 | 2.3 | 52 |
| 1 | 32 | demand | 30.4 | 9.3 / 26.4 | 1.45 | 4.1 | 42 |
| 1 | 64 | demand | 29.6 | 9.3 / 26.5 | 0.92 | 3.9 | 43 |
| 8 | 8 | demand | 36.0 | 4.2 / 10.3 | 10.9 | 17.6 | 35 |
| 8 | 8 | traced | 29.6 | 3.7 / 10.5 | 11.1 | 11.2 | 43 |
| 8 | 64 | demand | 33.7 | 3.8 / 9.9 | 10.3 | 15.5 | 38 |

Every 64-image burst downloaded the same **1.27 GB**. The 20-image runs show
the same shape: [raw/a1-20.json](raw/a1-20.json) and
[raw/a8-20.json](raw/a8-20.json).

**Attach phases** (64 images, serial attach, p50 / p95 ms, from
[raw/timing-a1-64.jsonl](raw/timing-a1-64.jsonl)):

| slot wait | load | receipt | NBD bind | prefetch start | mount | total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 7,115 / 15,075 | 12 / 33 | 5 / 10 | 129 / 455 | 0.7 / 2.7 | 14 / 36 | 7,275 / 15,288 |

There were no metadata waits: production images carry no metadata hints. With
8 slots, trace prefetch jobs queued for up to 15 s, against 1.1 s with serial
attach.

**Raw registry reads from the worker** (no NBD or cache; random aligned ranges
of production component blobs; MB/s with p50 latency):

| range | blobs | 8 in flight | 32 in flight | 64 in flight |
| --- | --- | ---: | ---: | ---: |
| 256 KiB | warm (just read) | 48.8, 56 ms | 49.1, 247 ms | 48.2, 349 ms |
| 256 KiB | cold | 26.5, 88 ms | 27.6, 332 ms | 26.0, 496 ms |
| 1 MiB | warm | 184.6, 57 ms | 191.7, 257 ms | |
| 4 MiB | warm | 734.3, 44 ms | 748.5, 217 ms | |
| 4 MiB | cold (other images) | 404.8, 97 ms | 403.5, 406 ms | |

- **Request rate.** The registry served about 180 requests/s whatever the size.
  During the probe the gateway's CPUs showed 67–77% iowait, with the registry
  process at 45–70% CPU. The likely cause is per-request filesystem lookups on
  the network Volume.
- **Errors.** About 5–10% of probe requests failed, where a random offset
  crossed a small blob's end. They are counted and excluded.

## Reading it

1. **Attach is not the cost.** It is about 170 ms of work per component, and
   NBD bind is 75% of that. Serial attach only meters starts into a fixed
   fetch rate.
2. **Parallel attach moves the wait into first commands and loses overall.**
   Every sandbox misses on demand at once against the same rate, and trace
   prefetch competes too. That explains the 0.8.3 regression; metadata
   prefetch does not, because production has no hints.
3. **The miss limit is not the cap.** Raising it from 8 to 64 left throughput
   at 42–43 MB/s and only lengthened each miss.
4. **The cap is the registry's per-request cost at 256 KiB per request.** Each
   node gets about 45 MB/s, shared across nodes. The 512 baseline saw about
   90 MB/s across three nodes.
5. **Fix: fetch larger windows per miss.**
   - Contiguous uncached chunks of the same component blob go in one range,
     up to 2–4 MiB. EROFS component blobs are contiguous 256 KiB chunks, so
     neighbours coalesce trivially.
   - Trace replay goes in coalesced runs.
   - The chunk store's RAFS path already does this (1 MiB demand windows, 4 MiB
     store-node extents).
   - Expected: several times the per-node rate, bounded by Volume bandwidth
     (about 400 MB/s cold) and gateway NIC. Fetched bytes rise somewhat, from
     3% today.
   - Then re-measure attach concurrency, since the contention behind 0.8.3
     should shrink with it.
