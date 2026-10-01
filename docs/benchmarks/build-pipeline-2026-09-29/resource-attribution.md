# Resource attribution

Reproduce with `python3 docs/benchmarks/build-pipeline-2026-09-29/host-attribution.py`. The JSON binds the exact input files. Candidate results require telemetry covering their windows; zero coverage is missing evidence. CPU units are occupied cores. Disk busy counters are excluded.

| Phase | Wall s | Gateway coverage | Host / API / registry / driver mean cores | Registry written GiB | I/O-weighted await ms | I/O PSI mean / p95 % |
|---|---:|---:|---:|---:|---:|---:|
| slotq-a-cold | 305.181 | 99.6% | 0.353 / 0.068 / 0.143 / 0.046 | 14.198 | 47.682 | 14.438 / 61.068 |
| slotq-a-condition | 119.057 | 97.4% | 0.367 / 0.079 / 0.115 / 0.078 | 0.621 | 3.802 | 9.666 / 39.759 |
| slotq-a-warm | 17.578 | 91.0% | 1.384 / 0.123 / 0.454 / 0.617 | 0.204 | 2.249 | 34.596 / 50.399 |
| slotq-b-cold | 291.265 | 99.6% | 0.376 / 0.069 / 0.150 / 0.048 | 14.209 | 45.411 | 15.555 / 67.758 |
| slotq-b-warm | 17.195 | 81.4% | 1.594 / 0.129 / 0.545 / 0.688 | 0.206 | 3.017 | 44.723 / 50.298 |

## Cold-arm busiest 30-second windows

Each host selects its own busiest CPU window; the gateway row selects its busiest I/O PSI window. These rows must not be added as a simultaneous fleet total.

| Phase / host | Window start UTC | CPU mean | BuildKit / dockerd mean cores | CPU / I/O PSI mean % | Disk write MiB/s | I/O-weighted await ms |
|---|---|---:|---:|---:|---:|---:|
| slotq-a-cold / builder-168008406 | 2026-09-29T21:00:33.761729+00:00 | 5.813 | 4.756 / 0.005 | 11.018 / 2.610 | 52.095 | 3.136 |
| slotq-a-cold / builder-168008407 | 2026-09-29T20:57:41.413695+00:00 | 5.855 | 4.857 / 0.004 | 14.043 / 1.353 | 35.882 | 2.910 |
| slotq-a-cold / builder-168008410 | 2026-09-29T20:57:47.654765+00:00 | 5.912 | 4.859 / 0.003 | 12.058 / 0.482 | 16.378 | 1.928 |
| slotq-a-cold / builder-168008411 | 2026-09-29T20:57:39.546102+00:00 | 5.930 | 4.952 / 0.006 | 14.518 / 1.458 | 35.787 | 2.989 |
| slotq-a-cold / gateway | 2026-09-29T20:59:00.919963+00:00 | 0.652 | 0.000 / 0.007 | 2.859 / 38.318 | 134.417 | 76.831 |
| slotq-b-cold / builder-168008406 | 2026-09-29T21:10:13.761693+00:00 | 6.873 | 4.551 / 1.357 | 32.230 / 3.677 | 81.251 | 3.080 |
| slotq-b-cold / builder-168008407 | 2026-09-29T21:08:31.413692+00:00 | 6.134 | 5.422 / 0.000 | 18.360 / 0.644 | 0.787 | 0.355 |
| slotq-b-cold / builder-168008410 | 2026-09-29T21:10:01.654781+00:00 | 6.563 | 3.717 / 1.545 | 30.415 / 6.465 | 93.974 | 2.033 |
| slotq-b-cold / builder-168008411 | 2026-09-29T21:10:05.546779+00:00 | 6.488 | 3.607 / 1.571 | 29.163 / 6.341 | 147.025 | 2.362 |
| slotq-b-cold / gateway | 2026-09-29T21:09:38.919958+00:00 | 0.763 | 0.000 / 0.009 | 5.343 / 46.324 | 151.831 | 71.916 |

## Cold publication attribution

All times below are per-build means in seconds. Missing pull measurements stay N/A. Nested timings cannot be added to their parent publication total.

| Phase / recipe | Publication | Docker pull (reporting records) | Selective child | Squash | mkfs | Component publish | Finishing wait |
|---|---:|---:|---:|---:|---:|---:|---:|
| slotq-a-cold / python-agent | 43.897 | 38.362 (16) | N/A | 0.928 | 1.870 | 0.724 | N/A |
| slotq-a-cold / typescript-multistage | 0.760 | N/A (0) | 0.411 | 0.004 | 0.041 | 0.164 | N/A |
| slotq-a-cold / typescript-tools | 11.228 | N/A (0) | 5.994 | 1.688 | 1.157 | 2.090 | N/A |
| slotq-b-cold / python-agent | 39.608 | 33.817 (16) | N/A | 0.858 | 1.865 | 1.055 | 0.166 |
| slotq-b-cold / typescript-multistage | 0.703 | N/A (0) | 0.481 | 0.004 | 0.042 | 0.142 | 1.618 |
| slotq-b-cold / typescript-tools | 11.626 | N/A (0) | 6.495 | 1.938 | 1.305 | 1.593 | 0.803 |

## Cold completion tails

Builder ranges compare per-host means within the stated window; they are not percentiles.

| Phase / window | Gateway CPU mean | Registry writes MiB/s | Registry await ms | Builder CPU mean range | Builder dockerd mean range |
|---|---:|---:|---:|---:|---:|
| slotq-a-cold / completion_tail_60s | 0.185 | 37.826 | 62.153 | 1.072–1.837 | 0.740–1.293 |
| slotq-a-cold / post_completion_60s | 0.063 | 0.028 | 4.547 | 0.124–0.166 | 0.000–0.000 |
| slotq-b-cold / completion_tail_60s | 0.193 | 47.097 | 74.933 | 0.316–2.019 | 0.003–1.251 |
| slotq-b-cold / post_completion_60s | 0.052 | 0.029 | 3.696 | 0.111–0.142 | 0.000–0.000 |

The JSON also includes per-host CPU attribution, memory/PSI and OOM/swap counters. No health probe was configured; these files supply no sampled health-success evidence.

Limits:

- Local read-only analysis; no new samples or production requests.
- Only complete raw counter intervals are used; boundary time is excluded.
- Busy 30-second windows are selected separately per host/metric, not simultaneous fleet totals.
- Disk busy counters are deliberately excluded; await is weighted by completed I/O counts.
- Last 60 seconds may contain fewer builds; post 60 seconds includes writeback and any unrelated activity.
- Timing subphases are nested and overlap; do not add environment counters to their parent total.
- Cold means invalidated dependency RUN, not uncached base images, package downloads or empty disks.
- No health probe was configured in these samplers; zero probes is missing evidence, not health success.
