# Cache-affinity performance comparison

The seed is a cache-population phase. The repeat has completed; measured comparisons follow.

The repeat completed in **98.594s versus 115.495s** (-14.63%). Client p95 changed -13.23%. This is a measured improvement for this repeat workload, with mixed tail results: build/push p95 changed +15.09% and environment p95 +13.67%. It does not establish that every build is faster.

All included phases completed 48/48 builds using exactly the same 48 frozen context SHA-256 values, recipe names, and variants.

| Metric | baseline | seed | repeat |
| --- | ---: | ---: | ---: |
| Batch wall, seconds | 115.495 | 125.635 | 98.594 |
| Client p95, seconds | 112.104 | 116.639 | 97.275 |
| Submission/admission p95, seconds | 79.793 | 88.266 | 65.685 |
| Build/push p50, seconds | 27.074 | 32.266 | 21.647 |
| Build/push p95, seconds | 44.012 | 47.592 | 50.656 |
| Environment p95, seconds | 3.738 | 3.992 | 4.249 |
| Build/push aggregate elapsed, seconds | 1293.821 | 1445.475 | 1024.204 |
| Environment aggregate elapsed, seconds | 94.849 | 98.935 | 78.924 |

Aggregate phase elapsed time sums overlapping builds. It is neither CPU time nor an additive decomposition of the batch wall time. Cache preparation and pre-mounting are already included in build/push.

## BuildKit execution versus materialization

The table reports executed application instructions separately from layer downloads/extraction. Export timings are complete parent vertices; their nested operations are not added again.

| Phase | Recipe | Application RUN executions | Mean seconds per execution | Materialization mean per build | Image export mean | Cache export mean |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| baseline | python-agent | 15 | 3.620 | 11.906 | 0.581 | 0.481 |
| baseline | typescript-tools | 13 | 30.077 | 2.812 | 1.137 | 0.300 |
| baseline | typescript-multistage | 15 | 28.767 | 4.750 | 0.356 | 0.356 |
| seed | python-agent | 15 | 3.773 | 17.100 | 0.806 | 0.375 |
| seed | typescript-tools | 14 | 27.736 | 5.838 | 1.469 | 0.556 |
| seed | typescript-multistage | 15 | 28.207 | 8.112 | 0.750 | 0.581 |
| repeat | python-agent | 5 | 3.900 | 4.750 | 0.406 | 0.306 |
| repeat | typescript-tools | 12 | 30.108 | 3.481 | 1.075 | 0.406 |
| repeat | typescript-multistage | 11 | 31.855 | 4.519 | 0.238 | 0.225 |

A cached instruction can still take time to materialize its output on a fresh builder. Its header may say RUN while its progress is transferring/extracting layers. These are not repeated dependency installations. Shared downloads and concurrent vertices mean materialization sums are not independent physical work.

## Last client completion

- baseline: case 007, typescript-tools/app-change-7; client 115.483s, submission/admission 79.267s (30 retryable 503 responses), build/push 31.899s, environment 3.271s.
- seed: case 004, typescript-tools/app-change-6; client 125.625s, submission/admission 94.340s (36 retryable 503 responses), build/push 27.498s, environment 2.995s.
- repeat: case 037, typescript-tools/app-change-17; client 98.581s, submission/admission 62.301s (22 retryable 503 responses), build/push 31.566s, environment 3.747s.

Last completion is selected by precise client finish offset, not truncated server timestamp. Client polling and transport can make the last observed client differ from the last server completion.

## Interpretation limits

The baseline and seed show broadly similar application execution counts. Their difference is therefore not evidence of exact-context reuse yet. The seed starts without this cohort's new affinity tags and establishes the cache population used by the repeat.

The repeat still executes application instructions despite selecting matching affinity tags. Its Python compile/smoke execution observations fall from 15 to 5; TypeScript tools from 13 to 12; multistage from 15 to 11. Cached markers and execution counts describe vertices and can overlap within a build, so they must not be added as disjoint build counts. Exact selection is not proof that BuildKit reused every corresponding record.

The standalone controlled proof tests an old exact result outside the eight recency-selected imports on independent empty BuildKit stores, and verifies source/ARG invalidation. That establishes the mechanism separately from these sequential fleet observations. Placement, cache history, volume state and scheduling can also change batch time; one repeat is not a confidence interval or a universal customer-workload forecast.

See [numeric report](performance-comparison.json), [baseline progress analysis](baseline-buildkit-progress.md), and [controlled proof protocol](CONTROLLED-PROOF.md). Reproduce with `python3 docs/benchmarks/build-cache-affinity-2026-09-29/performance-report.py`; it reads only local receipts and rewrites these two derived report files.
