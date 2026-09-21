# 128 sandboxes, concurrency 8: retained failure analysis

Run `density-902eed08ef744fc5` failed eleven wake admissions. The evidence again supports testing a short CPU-pressure wait with unchanged thresholds: sustained CPU use was low, while adjacent requests encountered the same brief rejection period. This run predates deployment of that wait and does not establish that it fixes the failure.

Sources: [raw benchmark](sandbox-density-128-acl-c8-deep-2026-09-07.json), [analysis and wake CPU intervals](sandbox-density-128-acl-c8-analysis-2026-09-07.json), [original host samples, gzip](sandbox-density-128-acl-c8-host-2026-09-07.jsonl.gz).

The profile was 128 sandboxes on 32 vCPUs, each with a 1-CPU limit, 1 GiB memory limit, 256 MiB randomly initialized resident memory and a rotating 16 MiB dirty subset. Every action fully verified and hashed resident memory. Concurrency was 8; three cycles were requested, but the first wake phase failed.

| Phase | Success / error | Service p95 | Queued completion p95 | Whole wave |
| --- | ---: | ---: | ---: | ---: |
| Act | 128 / 0 | 0.631 s | 8.558 s | 9.058 s |
| Park | 128 / 0 | 1.397 s | 13.498 s | 14.795 s |
| Wake | 117 / 11 | 1.732 s | 17.724 s | 18.333 s |

Percentiles use raw successful-operation durations; errors remain separate. Park exceeded the 10-second completion limit. Wake exceeded that limit and the 15-second wave limit, in addition to the eleven failures. The older benchmark report omitted its `slo_violations` field when a phase raised; these failures are independently recomputed and retained in the linked analysis. All 128 create and park operations succeeding is not a complete performance pass.

During wake, 17.002 seconds of fully contained host intervals show mean CPU execution of 25.86%, a one-second maximum of 37.73%, and 2.82% I/O wait. The interval following the CPU maximum fell to 19.48%. CPU PSI `some` was 7.38% and memory PSI `full` was 1.09%, calculated from cumulative stall counters. No wake OOM, swap-in or swap-out occurred.

Failed IDs 98–108 lie between neighboring successful request starts at 13.853 and 14.223 seconds into wake. This is an approximate bracket from sequential queue progress; individual errors have no timestamps. The encompassing one-second CPU interval was the 37.73% maximum. These observations support a transient spike and cached rejection explanation, but cannot resolve or disprove the admission sampler's 50 ms measurement. They do not justify relaxing its CPU threshold.

The host sampler restarted during create: its first sample is **23:10:09.463 UTC**. Baseline, single-create, single-act and the first 23.055 seconds of the 79.050-second create phase have no host coverage. Create counter intervals cover 55.006 seconds. Act, park and wake are covered; intervals crossing phase boundaries are excluded. Actual host cadence was approximately one second, and the longest collection took 0.209 seconds.

Three complete samples each matched 128 distinct boot processes and disjoint cgroups. All 128 sampled leaf CPU limits remained `100000 100000` (1 CPU). Cgroup memory charges totaled 35.83–35.95 GiB, including roughly 33.5 GiB of file charges. Boot-process PSS totaled 1.85–1.96 GiB; this process subset is not the total physical memory of the guests. PSS, RSS and cgroup charges are separate views and must not be added. Likewise, `file`, `shmem` and `file_mapped` overlap. Large `MemAvailable` values include reclaimable mapped file cache.

The host had 96 GiB configured swap, with 772 KiB used during wake. Earlier runs without host swap are not directly equivalent. Final inventory was complete with zero active sandboxes or creates; cleanup errors and remaining owned IDs were empty.
