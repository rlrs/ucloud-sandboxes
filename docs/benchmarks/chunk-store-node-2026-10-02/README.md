# Chunk store node benchmarks (C2.6), 2026-10-02

Two local questions for `ucloud-chunk-store`
([design](../../chunk-store-design.md#c26-store-node-as-built)): **how to serve**
1 MiB range GETs from a warm cache to a 500-sandbox burst, and **what to fill**
from S3 on a miss, given S12's latency
([S12](../s3-chunk-spike-2026-10-02/README.md)). Everything ran on one machine,
with no remote hosts.

## Answer

- **Serve from one asyncio loop with sendfile(2).** It moved 3.7–3.9 GiB/s of
  1 MiB ranges from page cache at concurrency 64–256, with p99 25 / 56 /
  101 ms, on one core. Thread-per-connection (`ThreadingHTTPServer`) moved the
  same bytes with about twice the p99 and two cores, and took up to 2.1 s to
  accept a 256-connection burst, because each accept waits for a busy GIL; the
  loop accepted it within 121 ms. aiohttp's `FileResponse` reached 2.1 GiB/s.
  nginx was not measured: it is not installed here, and installing it would
  have meant a remote fetch.
- **Fill aligned 4 MiB extents, and hedge only stalled GETs.** In a cold
  500-sandbox burst against an S3 stand-in with S12's latency shape, 4 MiB
  extents finished the burst fastest and had the lowest cold-start p99 (median
  of 3 runs: a 20.4 s burst, 17.4 s cold-start p99). A 1 MiB window spans
  two 1 MiB extents at most offsets, so 1 MiB extents need 2.4× the GETs and
  more reads wait on S3's time to first byte; 8–64 MiB extents (64 MiB is a
  whole pack) move 3.1–3.3× the bytes the readers need, in a burst that is
  bandwidth-bound. At 4 MiB, hedging cut the cold-start p99 from 29.0 s to
  17.4 s and the per-read p99 from 5.6 s to 1.2 s.
- **Hedge on progress, not elapsed time.** The first version hedged a GET that
  had not *finished* after a size-based allowance, as S12 tried; in a
  bandwidth-bound burst it duplicated flowing transfers (365 hedges at 4 MiB,
  amplification 2.65). Hedging only a GET with no first byte, or no body
  bytes, for 3× the median TTFB cut that to 108 hedges, at 2.4× and with a
  lower tail.
- **Warm the foundations first (C9.3).** Filling the run's foundation packs
  before the burst (2.5 GiB in 10.5 s at the modelled 250 MiB/s) took the
  burst from 20.4 s to 12.4 s and the cold-start p99 from 17.4 s to 6.2 s. A
  cold burst is bound by S3 bandwidth, not by the node.

## Machine and method

30 vCPUs, 78 GB RAM, Linux 5.15, CPython 3.10.13 (uv). Clients, servers and
the S3 stand-in are separate processes on loopback, so these numbers bound the
node's software, not a Hetzner NIC or the private network (not measured).
[`bench_chunk_store_node.py`](bench_chunk_store_node.py) runs both parts;
[`threaded_baseline.py`](threaded_baseline.py) is the first HTTP layer, kept
for comparison.

### Serving ([`serve.json`](serve.json))

- 64 objects of 64 MiB (4 GiB), filled through the warm API from a stand-in
  with no latency, then read once so they sit in page cache.
- Clients: 16 processes, each with persistent connections, issuing random
  4 KiB-aligned 1 MiB ranges for 15 s at concurrency 64, 128 and 256. The
  first request on each new connection is reported separately (it includes
  the accept).

| Server | Conc | MiB/s | p50 ms | p99 ms | max ms | First request p99 ms | Server cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **asyncio + sendfile (ships)** | 64 | 3,890 | 15.5 | 24.7 | 59.1 | 44.7 | 0.99 |
| | 128 | 3,877 | 31.1 | 55.8 | 86.6 | 41.1 | 1.00 |
| | 256 | 3,719 | 65.0 | 100.7 | 120.9 | 120.7 | 1.00 |
| threads + sendfile | 64 | 3,979 | 14.5 | 42.5 | 83.3 | 188.8 | 2.05 |
| | 128 | 3,870 | 29.7 | 87.5 | 160.4 | 426.0 | 2.04 |
| | 256 | 3,733 | 58.8 | 175.5 | 312.9 | 2,048.0 | 2.10 |
| threads + copy | 64 | 4,462 | 13.2 | 35.9 | 82.2 | 150.7 | 3.36 |
| | 128 | 4,387 | 26.8 | 73.7 | 143.1 | 465.9 | 3.34 |
| | 256 | 4,155 | 55.2 | 154.6 | 263.9 | 1,539.2 | 3.39 |
| aiohttp `FileResponse` | 64 | 2,192 | 28.8 | 36.7 | 108.5 | 68.6 | 1.15 |
| | 128 | 2,066 | 59.3 | 112.1 | 188.4 | 70.8 | 1.12 |
| | 256 | 2,168 | 116.3 | 130.0 | 157.3 | 113.1 | 1.14 |

- No errors in any run. At this throughput the p50 is queueing (Little's law:
  256 MiB in flight at 3.7 GiB/s is about 68 ms), so the tail is what differs.
- The loop is saturated at one core. A Cloud NIC is far below 3.7 GiB/s, so
  that is headroom; several loops on `SO_REUSEPORT` would be the next step.
- The threaded server's accept latency was first worse (p50 1.5 s, max 3.1 s
  at 256), because `Thread.start()` waits for the new thread to run.
  `_thread.start_new_thread` halved it; a 1 ms GIL switch interval did not
  help and cost a third of the throughput.

### Filling ([`fill.json`](fill.json), [`fill-r2.json`](fill-r2.json), [`fill-r3.json`](fill-r3.json); S3 slots: `fill-slots-r{1,2,3}.json`)

**Workload:** C2.6's budget case, 64 tasks × 8 rollouts on 3 workers. The 64
images come in 8 foundation groups of 8; a foundation has 10 packs of 32 MiB
and an image 6 packs of its own of 8 MiB. A cold start reads 25 windows of
1 MiB at 4 KiB-aligned offsets (70% in foundation packs), one after another,
like the worker's demand misses. A worker's node cache dedupes an image's
rollouts, so the store sees 3 readers per image: 192 readers, 4,800 reads,
1,600 distinct windows, starting within 10 s.

**S3 stand-in** (S12's shape; time-scaled by 0.2 so a run takes seconds, and
every latency below is scaled back):

- time to first byte: lognormal around 55 ms for 90% of GETs, 0.2–1.5 s for
  8%, 3–15 s for 2%;
- 100 MB/s per stream and 250 MiB/s in total (design §2's per-worker
  assumption; S12 saw at most 131 MiB/s from one client);
- a mid-body stall of 2–10 s with probability 1% per 8 MiB.

For one 1 MiB GET that gives p50 / p95 / p99 of 68 / 1,052 / 9,023 ms, inside
S12's measurements (60 / 808 / 5,495 ms at concurrency 32, 70 / 4,366 /
13,957 at 64). Objects are served under `meta/*.boot.zst` keys, which the
node does not check against their names; everything else is the shipped code.

**Results** (median of 3 runs; burst and cold-start waits in seconds, windows
in ms):

| Fill | Burst | Cold start p50 | Cold start p99 | Window p50 | Window p99 | Window max | S3 GETs | S3 bytes / needed | Hedges | Runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 40.0 | 8.9 | 28.2 | 173.3 | 8,924.8 | 14,961.6 | 4,800 | 3.0 | 0 | 3 |
| 1 MiB | 30.0 | 21.9 | 26.0 | 895.4 | 1,530.5 | 2,189.9 | 2,553 | 1.5 | 259 | 3 |
| 4 MiB | 20.4 | 13.3 | 17.4 | 523.6 | 1,243.6 | 1,826.8 | 1,047 | 2.4 | 108 | 3 |
| 4 MiB, no hedge | 32.2 | 12.4 | 29.0 | 396.7 | 5,590.4 | 15,320.7 | 949 | 2.4 | 0 | 3 |
| 8 MiB | 22.2 | 15.4 | 20.6 | 574.7 | 1,796.0 | 3,397.5 | 660 | 3.1 | 78 | 3 |
| 16 MiB | 23.4 | 16.7 | 21.9 | 439.4 | 3,943.9 | 6,805.2 | 498 | 3.2 | 74 | 3 |
| 64 MiB | 24.5 | 17.5 | 23.3 | 425.6 | 6,415.5 | 10,229.5 | 424 | 3.3 | 79 | 3 |
| 64 MiB, no hedge | 28.9 | 14.5 | 24.4 | 325.2 | 5,553.5 | 12,736.8 | 351 | 3.0 | 0 | 3 |
| 4 MiB, foundations warmed | 12.4 | 4.2 | 6.2 | 153.2 | 527.6 | 1,061.4 | 1,129 | 2.6 | 112 | 3 |
| 64 MiB, foundations warmed | 12.4 | 4.4 | 6.8 | 163.4 | 622.0 | 1,054.9 | 397 | 3.1 | 51 | 3 |
| 4 MiB, 128 S3 slots | 21.2 | 13.9 | 18.4 | 525.2 | 1,317.8 | 1,832.2 | 1,042 | 2.4 | 104 | 3 |
| 4 MiB, 32 S3 slots | 21.1 | 13.8 | 17.9 | 529.0 | 1,284.3 | 1,685.3 | 1,044 | 2.4 | 101 | 3 |

- **direct** is M1's Phase A: workers range-read S3 themselves, 1 MiB windows,
  no node. Each worker fetches its own copy, hence 3.0× the bytes. Its median
  read and median cold start are the lowest, since a miss waits for only
  1 MiB; its tail is S3's (window p99 8.9 s), and the burst takes twice as
  long as through the node.
- **The node trades median for tail.** A miss there waits for a 4 MiB extent,
  so the median cold start is 13.3 s against 8.9 s direct, but the p99 is
  17.4 s against 28.2 s and the burst ends in half the time. Warming removes
  most of that wait.
- **S3 slots do not matter here:** 32, 64 and 128 concurrent GETs give the
  same results, since the burst is bandwidth-bound. Production keeps 64.
- "Cold start" is one reader's total wait over its 25 reads; "window" is one
  read. Both are dominated by the S3 bandwidth cap: 1,600 MiB of distinct
  windows at 250 MiB/s is 6.4 s of transfer before any amplification.
- With the foundations warmed, a window p50 of about 140 ms is mostly the
  benchmark: a hit takes milliseconds (see Serving), but 192 reader threads
  in one Python process, times the 5× unscaling, inflate it.
- [`fill-elapsed-hedging.json`](fill-elapsed-hedging.json) is the first run,
  with hedging on elapsed time (`3 × median TTFB + size / 32 MiB/s`).

## Reproduce

```bash
uv run python docs/benchmarks/chunk-store-node-2026-10-02/bench_chunk_store_node.py serve --client-processes 16 --out serve.json
uv run python docs/benchmarks/chunk-store-node-2026-10-02/bench_chunk_store_node.py fill --out fill.json
uv run python docs/benchmarks/chunk-store-node-2026-10-02/bench_chunk_store_node.py fill --variants 4:2:c128 4:2:c32 --out fill-slots-r1.json
uv run python docs/benchmarks/chunk-store-node-2026-10-02/bench_chunk_store_node.py summarize fill.json fill-r2.json fill-r3.json
```

## Not measured

- A Hetzner NIC and the private network between workers and the node, and
  S3 from the node's own public IPv4. The first gate run measures them.
- nginx serving the cache directory.
- Several store nodes, and a node restart in the middle of a burst.
