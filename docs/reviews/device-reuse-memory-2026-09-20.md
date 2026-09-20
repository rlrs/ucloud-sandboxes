# Device reuse and memory reclaim qualification — 2026-09-20

Runtime 0.5.66 replaces immediate idle-device eviction with short retention and
raises the sandbox VM background-reclaim watermark. SDK 0.4.23 is unchanged.

## Problem and changes

The previous native pool discarded every returned block device above its high
watermark (16 in production). A park burst therefore destroyed devices that the
next resume burst immediately needed. Under memory fragmentation, recreating
blk-mq queues could fail even with substantial MemAvailable. Production worker
12397040 survived one such prewarm failure at 15:07:51 UTC. This is evidence of
avoidable allocation pressure, not proof that it caused the lost VMs.

`agentenv-device-reuse.patch` keeps returned devices for at least 60 seconds,
then a five-second reaper trims expired surplus above the steady-state target.
There is no new active-device admission ceiling. Exact-size devices are chosen
before resize candidates. Background refill maintains only the low watermark
(two devices), lets concurrent returns satisfy that demand, and backs off on
allocation failure while holding the single-flight guard. Existing exclusive
ownership, mount cleanup, and quarantine checks remain intact.

Sandbox init sets `vm.watermark_scale_factor=100`, increasing the distance
between free-page watermarks from the Linux default 0.1% to 1% of zone memory.
This gives background reclaim more headroom when buffered writes fill page
cache. It does not reserve 1% per sandbox, impose a memory limit, flush caches,
or disable compaction. Builders and the gateway do not receive this setting.
See the [kernel VM documentation](https://docs.kernel.org/admin-guide/sysctl/vm.html#watermark-scale-factor).

The earlier heartbeat `WorkingDirectory=/` fix is included in this release.
It avoids scanning shared virtiofs directories when the heartbeat imports Python.

## Isolated worker experiment

Worker 12397076, deployment `perf-memory-20260920`, had 32 vCPUs, 90 GB memory,
and a 440 GiB XFS loop filesystem on its guest ext4 disk. It had no production
workloads. Kernel and resource samples were streamed to the gateway so evidence
would survive a VM loss. No poweroff occurred.

The density workload was `scripts/benchmark_sandbox_density.py`, with additional
phase logging in the test copy (its default park mode was unchanged):

```text
--count 64 --concurrency 16 --create-concurrency 8 --cycles 6
--resident-mb 64 --dirty-mb 16 --memory-mb 512 --cpu-ms 10
```

The fixture verifies a persistent process nonce, memory hashes, filesystem data,
an open SQLite connection, and a Unix socket after each resume. Baseline 0.5.65
ran first, followed by the candidate native binary after an idle service restart.
Both used the original memory watermark of 10, isolating the device patch from
the proposed sysctl change.

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Lifecycle or cleanup errors | 0 | 0 |
| Devices discarded during measured run | 448 | 2 |
| Devices immediately after cleanup | 16 | 66 |
| Devices after 70 seconds idle | — | 16 |
| Direct allocation stalls | 2,372 | 0 |
| Compaction stalls | 105 | 0 |
| Sum of measured phase times | 85.17 s | 81.33 s |
| Sum of park phase times | 19.67 s | 19.04 s |
| Sum of wake phase times | 37.03 s | 36.29 s |

Delayed trimming retired the 50 idle excess devices as intended. The candidate passed the
fixture's latency targets; the baseline exceeded wake p95 3 s once and park p95
2 s once. This single fixed-order comparison demonstrates reduced churn, but
cache state and the service restart confound attribution of the modest speed
change and allocation-stall reduction. It is not a 512-sandbox production SLO.

## Memory experiment

A first random-I/O test was dominated by cache warming and is not used to claim
improved throughput. The repeated streaming test wrote fresh 128 GiB filesets
using four fio jobs, buffered io_uring, queue depth 16, 1 MiB sequential writes,
and end fsync. A prefill preceded alternating watermark values 10/100/10/100.
At most three filesets existed; obsolete sets were removed after each phase.
The original sysctl was restored and scratch files removed afterward.

| Phase | Watermark | Allocation stalls | Compaction stalls | Write MB/s | Write p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline 1 | 10 | 3,344 | 7,295 | 2,575 | 106 |
| Headroom 1 | 100 | 2,842 | 5,032 | 2,480 | 160 |
| Baseline 2 | 10 | 5,959 | 9,974 | 3,175 | 144 |
| Headroom 2 | 100 | 3,028 | 5,645 | 3,489 | 144 |

Across the two measured phases per setting, allocation stalls fell 37% and
compaction stalls 38%; pages scanned for compaction fell 75%. Throughput and
tail latency were mixed. The evidence supports reducing foreground reclaim
pressure, not a general throughput or latency guarantee. Ordering, cache state,
and storage variability remain limitations.

## Validation and evidence

- Full local check: 974 server tests (six platform skips), 118 SDK tests,
  lint, shell checks, Go contracts, wheel builds, and isolated install checks.
- Linux native daemon suite: 56 tests passed, including five new idle-pool
  regressions for repeated 64-device bursts, expiry, exact-size selection,
  shutdown/late returns, and bounded failure retry delay.
- [Machine-readable summary](../benchmarks/ucloud-device-reuse-memory-2026-09-20.json).
- Detailed private evidence is retained under the isolated deployment directory
  on the gateway; no client payloads are included in the committed summary.

These changes address observed guest-side allocation pressure and device churn.
They do not establish the cause of the earlier unexplained VM poweroffs, which
still require host-side termination evidence.
