# Build optimization: observed comparison

Historical baseline comparison; observed changes are not isolated causal speedups.

Baseline: `/home/alex-admin/ucloud-sandboxes/docs/benchmarks/registry-io-2026-09-29/io-repeat/summary.json`  
Candidate: `/home/alex-admin/ucloud-sandboxes/docs/benchmarks/builder-preparation-2026-09-29/prep-repeat/summary.json`

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Successful / cases | 48 / 48 | 48 / 48 |
| Batch seconds | 135.561 | 139.054 |
| Client p50 seconds | 76.800 | 87.754 |
| Client p95 seconds | 132.432 | 136.559 |
| Submission p50 seconds | 40.367 | 41.709 |
| Submission p95 seconds | 90.446 | 98.797 |
| Node queue p50 seconds | 0.004 | 0.004 |
| Node queue p95 seconds | 0.006 | 0.015 |
| Build/push p50 seconds | 31.155 | 34.215 |
| Build/push p95 seconds | 57.641 | 50.801 |
| EROFS publication p50 seconds | 4.196 | 3.021 |
| EROFS publication p95 seconds | 12.288 | 9.789 |
| Fleet peak admitted | 16 | 16 |
| Maximum per-owner admitted | 4 | 4 |
| Fleet peak executing | 16 | 16 |
| Maximum per-owner executing | 4 | 4 |
| HTTP 503 responses | 711 | 775 |
| Repeated submit attempts | 711 | 775 |
| History rows reported | 48 | 48 |
| Longest queue behind full owner with free peer (seconds) | 0 | 0 |
| Total queue behind full owner with free peer (seconds) | 0 | 0 |
| groups_reused total (records reporting) | 160 (48) | 160 (48) |
| groups_built total (records reporting) | 48 (48) | 48 (48) |
| erofs_bytes_built total (records reporting) | 383524864 (48) | 383520768 (48) |
| preflight_misses total (records reporting) | 48 (48) | 48 (48) |
| docker_pull_skipped total (records reporting) | 48 (48) | 48 (48) |
| selective_materializations total (records reporting) | 48 (48) | 48 (48) |
| selective_fallbacks total (records reporting) | unknown (0) | unknown (0) |
| oci_layers_materialized total (records reporting) | 240 (48) | 240 (48) |
| oci_download_bytes total (records reporting) | 96991532 (48) | 96993279 (48) |

## Candidate evidence gates

These gates do not replace the live semantic and resource checks.

- expected_cases: pass
- all_succeeded: pass
- unique_build_ids: pass
- complete_client_timings: pass
- expected_concurrency: pass
- expected_builder_owners: pass
- maximum_admitted_per_owner: pass
- maximum_executing_per_owner: pass
- durable_history_complete: pass
- queue_intervals_complete: pass

Same fixture hash set: True.

Exact per-owner peaks, build intervals, queue spans, cache-step evidence, HTTP categories,
environment distributions and failures are retained in the companion JSON.

## Interpretation limits

- This historical comparison is descriptive: context sets, cache warmth, fleet age and release changes can confound latency deltas.
- Admission is created_at to finished_at, including preparation; execution is execution_started_at to finished_at, including publication but excluding later cleanup.
- Nonzero queue time can reflect cleanup handoff. Qualification checks the four-nonterminal admission limit, not literal zero queue_wait_ms.
- An unused measured execution slot does not prove memory/disk/cache readiness or quantify avoidable client delay. Cross-owner timestamps require synchronized clocks.
- Gateway admission waiting is part of submission/client latency and is not included in the node queue intervals.
- Missing environment counters are unreported, not zero. OCI bytes count selected compressed descriptors, not measured physical registry I/O.
- HTTP 503 counts do not identify error_code when the harness did not capture response bodies. Retry counts alone do not decide acceptance.
- Durable history completeness uses the harness's per-build lookup count; this report does not independently query the database or validate row contents.
- This report does not establish real-sandbox correctness, source/bundle identity, host resource headroom, or fleet-wide production capacity.
