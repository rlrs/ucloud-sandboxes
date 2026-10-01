# Resource attribution

Reproduce with `python3 docs/benchmarks/registry-pulls-2026-09-30/host-attribution.py`. The JSON binds the exact input files. Candidate results require telemetry covering their windows; zero coverage is missing evidence. CPU units are occupied cores. Disk busy counters are excluded.

| Phase | Wall s | Gateway coverage | Host / API / registry / driver mean cores | Registry written GiB | I/O-weighted await ms | I/O PSI mean / p95 % |
|---|---:|---:|---:|---:|---:|---:|
| slotq-pulls-a-cold | 271.068 | 99.6% | 0.411 / 0.079 / 0.170 / 0.049 | 14.229 | 47.106 | 17.808 / 71.583 |
| slotq-pulls-b-cold | 246.499 | 99.0% | 0.416 / 0.075 / 0.179 / 0.053 | 14.193 | 47.989 | 17.662 / 72.547 |
| slotq-pulls-a2-cold | 275.437 | 98.8% | 0.374 / 0.073 / 0.159 / 0.044 | 14.182 | 47.280 | 15.965 / 66.157 |

## Cold-arm busiest 30-second windows

Each host selects its own busiest CPU window; the gateway row selects its busiest I/O PSI window. These rows must not be added as a simultaneous fleet total.

| Phase / host | Window start UTC | CPU mean | BuildKit / dockerd mean cores | CPU / I/O PSI mean % | Disk write MiB/s | I/O-weighted await ms |
|---|---|---:|---:|---:|---:|---:|
| slotq-pulls-a-cold / builder-168016286 | 2026-09-29T22:26:16.529002+00:00 | 6.251 | 3.819 / 0.965 | 23.112 / 2.857 | 96.358 | 1.426 |
| slotq-pulls-a-cold / builder-168016287 | 2026-09-29T22:27:14.623127+00:00 | 6.268 | 3.655 / 1.433 | 20.267 / 1.469 | 62.884 | 1.490 |
| slotq-pulls-a-cold / builder-168016314 | 2026-09-29T22:27:26.636678+00:00 | 7.138 | 4.792 / 1.370 | 41.559 / 1.437 | 37.525 | 2.821 |
| slotq-pulls-a-cold / builder-168016315 | 2026-09-29T22:27:14.573526+00:00 | 6.700 | 4.187 / 1.343 | 26.759 / 3.866 | 124.342 | 1.969 |
| slotq-pulls-a-cold / gateway | 2026-09-29T22:26:43.379690+00:00 | 0.623 | 0.000 / 0.006 | 2.853 / 46.657 | 151.829 | 76.342 |
| slotq-pulls-b-cold / builder-168016286 | 2026-09-29T22:36:52.528989+00:00 | 6.416 | 4.267 / 0.006 | 19.760 / 2.119 | 67.746 | 1.258 |
| slotq-pulls-b-cold / builder-168016287 | 2026-09-29T22:37:54.623110+00:00 | 6.529 | 4.053 / 0.015 | 21.360 / 2.313 | 102.218 | 1.532 |
| slotq-pulls-b-cold / builder-168016314 | 2026-09-29T22:37:50.636659+00:00 | 5.919 | 3.880 / 0.011 | 11.390 / 2.769 | 152.410 | 1.431 |
| slotq-pulls-b-cold / builder-168016315 | 2026-09-29T22:36:52.573169+00:00 | 6.201 | 4.553 / 0.005 | 17.809 / 2.780 | 45.748 | 0.848 |
| slotq-pulls-b-cold / gateway | 2026-09-29T22:37:29.379702+00:00 | 0.858 | 0.000 / 0.011 | 6.176 / 48.985 | 166.920 | 66.530 |
| slotq-pulls-a2-cold / builder-168016286 | 2026-09-29T22:48:06.529026+00:00 | 6.797 | 3.832 / 1.220 | 30.510 / 3.991 | 124.142 | 1.544 |
| slotq-pulls-a2-cold / builder-168016287 | 2026-09-29T22:48:04.623117+00:00 | 6.614 | 4.567 / 0.861 | 28.616 / 2.892 | 44.517 | 1.023 |
| slotq-pulls-a2-cold / builder-168016314 | 2026-09-29T22:48:04.636723+00:00 | 6.452 | 4.328 / 0.887 | 26.499 / 4.185 | 66.931 | 1.028 |
| slotq-pulls-a2-cold / builder-168016315 | 2026-09-29T22:48:04.573162+00:00 | 6.665 | 3.871 / 1.581 | 31.480 / 4.251 | 98.838 | 1.635 |
| slotq-pulls-a2-cold / gateway | 2026-09-29T22:47:43.379704+00:00 | 0.706 | 0.000 / 0.007 | 4.109 / 41.748 | 145.249 | 79.972 |

## Cold publication attribution

All times below are per-build means in seconds. Missing pull measurements stay N/A. Nested timings cannot be added to their parent publication total.

| Phase / recipe | Publication | Docker pull (reporting records) | Selective child | Squash | mkfs | Component publish | Finishing wait |
|---|---:|---:|---:|---:|---:|---:|---:|
| slotq-pulls-a-cold / python-agent | 44.004 | 37.598 (16) | N/A | 0.714 | 2.019 | 1.310 | 4.682 |
| slotq-pulls-a-cold / typescript-multistage | 1.424 | N/A (0) | 0.450 | 0.004 | 0.033 | 0.421 | 5.008 |
| slotq-pulls-a-cold / typescript-tools | 12.704 | N/A (0) | 6.870 | 1.973 | 1.351 | 2.537 | 4.986 |
| slotq-pulls-b-cold / python-agent | 15.594 | N/A (0) | 9.438 | 0.295 | 1.994 | 0.901 | 1.613 |
| slotq-pulls-b-cold / typescript-multistage | 0.942 | N/A (0) | 0.480 | 0.004 | 0.038 | 0.188 | 1.204 |
| slotq-pulls-b-cold / typescript-tools | 14.206 | N/A (0) | 6.756 | 1.947 | 1.389 | 2.832 | 0.002 |
| slotq-pulls-a2-cold / python-agent | 34.667 | 29.029 (16) | N/A | 0.819 | 2.057 | 0.711 | 0.885 |
| slotq-pulls-a2-cold / typescript-multistage | 1.159 | N/A (0) | 0.551 | 0.006 | 0.041 | 0.122 | 0.625 |
| slotq-pulls-a2-cold / typescript-tools | 11.972 | N/A (0) | 6.785 | 1.935 | 1.359 | 1.636 | 3.829 |

## Cold completion tails

Builder ranges compare per-host means within the stated window; they are not percentiles.

| Phase / window | Gateway CPU mean | Registry writes MiB/s | Registry await ms | Builder CPU mean range | Builder dockerd mean range |
|---|---:|---:|---:|---:|---:|
| slotq-pulls-a-cold / completion_tail_60s | 0.201 | 40.531 | 34.898 | 0.736–1.740 | 0.077–0.736 |
| slotq-pulls-a-cold / post_completion_60s | 0.053 | 0.016 | 4.260 | 0.093–0.139 | 0.000–0.000 |
| slotq-pulls-b-cold / completion_tail_60s | 0.308 | 82.065 | 69.494 | 0.527–2.721 | 0.000–0.001 |
| slotq-pulls-b-cold / post_completion_60s | 0.052 | 0.024 | 4.297 | 0.072–0.102 | 0.000–0.000 |
| slotq-pulls-a2-cold / completion_tail_60s | 0.189 | 45.355 | 74.544 | 0.092–1.940 | 0.000–0.788 |
| slotq-pulls-a2-cold / post_completion_60s | 0.054 | 0.014 | 4.128 | 0.085–0.169 | 0.000–0.000 |

The JSON also includes per-host CPU attribution, memory/PSI, OOM/swap counters and sampled HTTPS health latency. Gateway-local HTTPS probes are not external sandbox capacity tests.

Limits:

- Local read-only analysis; no new samples or production requests.
- Only complete raw counter intervals are used; boundary time is excluded.
- Busy 30-second windows are selected separately per host/metric, not simultaneous fleet totals.
- Disk busy counters are deliberately excluded; await is weighted by completed I/O counts.
- Last 60 seconds may contain fewer builds; post 60 seconds includes writeback and any unrelated activity.
- Timing subphases are nested and overlap; do not add environment counters to their parent total.
- Cold means invalidated dependency RUN, not uncached base images, package downloads or empty disks.
- Gateway-local HTTPS health probes validate TLS but are not external end-to-end sandbox capacity tests.
- The baseline arm ran first; shared package/base caches and host writeback may differ despite fresh per-case dependency nonces.
