# Single-import performance comparison

Both phases completed 48/48 builds with the same frozen contexts. Batch time changed from 98.594 to 54.656 seconds (-44.56%). Tail and phase changes below remain part of the result.

| Measurement | eight_imports | single_import |
| --- | ---: | ---: |
| Batch wall seconds | 98.594 | 54.656 |
| Client p95 seconds | 97.275 | 21.683 |
| Client maximum seconds | 98.581 | 54.637 |
| Submission p95 seconds | 65.685 | 19.522 |
| Build/push median seconds | 21.647 | 1.318 |
| Build/push p95 seconds | 50.656 | 2.355 |
| Build/push maximum seconds | 52.460 | 33.629 |
| Environment p95 seconds | 4.249 | 0.940 |
| Submit HTTP503 observations | 309.000 | 55.000 |

Application observations below describe retained request logs, not necessarily distinct worker executions. Cached materialization and nested exporter operations are not added to enclosing phases.

| Phase / recipe | Executed application vertex observations | Mean observed seconds | Layer materialization mean per build | Image export mean | Cache export mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| eight_imports / python-agent | 5 | 3.900 | 4.750 | 0.406 | 0.306 |
| eight_imports / typescript-tools | 12 | 30.108 | 3.481 | 1.075 | 0.406 |
| eight_imports / typescript-multistage | 11 | 31.855 | 4.519 | 0.238 | 0.225 |
| single_import / python-agent | 0 | N/A | 0.000 | 0.194 | 0.106 |
| single_import / typescript-tools | 1 | 19.900 | 0.725 | 0.250 | 0.119 |
| single_import / typescript-multistage | 0 | N/A | 0.019 | 0.194 | 0.144 |

Limits:

- The same 48 recipe/variant/context identities are verified; each pool starts with empty local BuildKit caches.
- Sequential fleet runs retain placement, registry cache-history, host cache and scheduling differences.
- Aggregate phase and vertex durations overlap; they are not independent CPU time or additive batch-wall components.
- Cache prepare/mount are included in build/push; child preparation stages are nested within environment publication.
- Application execution observations come from per-request progress and can include shared or replayed vertices.
- An affinity match is cache selection evidence; actual application RUN reuse is a separate observation.
- SDK 0.4.33 and frozen harness are unchanged; SDK process attribution was unavailable in the SSH-launched baseline.
- These synthetic build tests do not qualify running-agent capacity or establish a universal workload speedup.

Reproduce: `python3 docs/benchmarks/build-cache-single-import-2026-09-29/performance-report.py`.
