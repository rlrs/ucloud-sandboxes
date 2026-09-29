# Qualification claim and reproducibility review

This read-only review checked all six phase summaries, preserved fixture
manifests and locks, the benchmark/validation scripts, replacement smoke
receipts, and the final cache-prune receipt. No production settings were changed
by the review. Final fleet retirement, durable-history and health receipts are
maintained by the main run in [README.md](README.md).

## Verified evidence

All **138 recorded builds succeeded**, with 138 distinct build UUIDs and image
IDs. Every recorded context hash matches its recipe/variant entry in the
[90 preserved fixture manifests](fixture-manifests.json); the builds used 87
distinct contexts. The local fixture generator and all four preserved locks
match the hashes checked before staging:

| Input | SHA-256 |
| --- | --- |
| `scripts/build_load_fixtures.py` | `41462b527c7384d843c324eb40546850b236f396ab0a9ef65f143c439eaaa0ae` |
| `locks/node-base.json` | `adbf9fb62e8932ef402f41666c63bb16e7246723e4ffc4b056608a7766e5c540` |
| `locks/node-dependency-change.json` | `154e30e2bb8c06984268ce34ba63dccb9750066c695def003a9a4a2160796fd4` |
| `locks/python-base-resolved.txt` | `c49659c791e5cf940eabcd45c6dd4880c96dac203a60919df6fac4abb7d3d98d` |
| `locks/python-dependency-change-resolved.txt` | `0960cb43b6bcf2e08c99bd8fb2a111054293dfc567428eb4e1241c241755e9b7` |

Both Python locks freeze 124 package versions; only `rich` changes between them.
The npm locks include integrity hashes for all 500 nonroot packages; only `zod`
changes. Python requirements have no artifact hashes, so the evidence supports
frozen dependency versions and input contents, not guaranteed identical wheel
artifacts. The base pins identify top-level image digests; reproduction must
also preserve the `linux/amd64` platform used by these builders. Platform-specific
manifest digests and sanitized pip-report artifact hashes would strengthen
provenance for a stricter reproduction claim.

Context hashes cover the generator's selected paths and file contents, excluding
`fixture.json`; they are neither SDK archive hashes nor a claim of identical
filesystem timestamps or resulting OCI digests. The measured timestamp effects
are documented in [cache-miss-analysis.md](cache-miss-analysis.md).

## Replacement cohorts

[replacement/summary.json](replacement/summary.json) records 24/24 successful
builds on four replacement VMs, a 149.107-second batch and 24 durable-history
records. The initial-store logs record 0 B for each replacement BuildKit store.
The shared registry and EROFS caches remained populated after the first prune.

Values below are seconds, except EROFS group counts. Percentiles use linear
interpolation over each cohort's 12 observations, matching the report scripts.

| Cohort | Client p50 / p95 | Build + push p50 / p95 | EROFS publication p50 / p95 | EROFS groups reused / built |
| --- | ---: | ---: | ---: | ---: |
| Older, application revisions 1–4 | 96.840 / 149.096 | 35.594 / 53.758 | 25.528 / 69.090 | 40 / 12 |
| Recent, application revisions 25–28 | 96.891 / 128.337 | 33.959 / 52.931 | 15.758 / 61.570 | 42 / 10 |

To reproduce the split, select summary records by the numeric suffix of
`variant`: 1–4 versus 25–28. Use `client_wall_seconds`, and divide
`build.timings.phases.docker_build_and_push_ms` and `immutable_environment_ms`
by 1,000. Sum `build.timings.environment.groups_reused` and `groups_built`.

These results establish successful recovery after builder replacement and
bounded cache pruning. They do not establish instant warm performance, isolate
the cost of pruning, or measure a cache-disabled counterfactual. VM state and
retained tags changed together; individual requests select only eight imports.
Placement, within-phase sharing and parent-chain selection also affect latency.
Cold/warm throughput ratios are likewise uncontrolled because concurrency and
repeated-context counts differ.

## Script accounting and validation limits

- [The runner](../../../scripts/live_build_load_benchmark.py) measures SDK
  packaging/upload, admission and polling in client latency. HTTP event timers
  end at response headers; they are not full response-transfer durations.
- Worker execution intervals include build/push and EROFS publication. They
  are not counts of simultaneously executing Dockerfile `RUN` instructions.
  Cross-host overlap depends on synchronized clocks; telemetry windows use
  second-resolution client timestamps.
- `CACHED` identifies an observed vertex, not an entirely cached request.
  Missing log evidence is unknown. Identical graphs can share concurrent work
  despite unique image names and build IDs. Retries/503s do not imply duplicate
  execution; the harness does not retain their error bodies.
- [The inventory tool](../../../scripts/build_load_qualify_images.py) counts
  compressed descriptors and EROFS payloads, not unpacked roots or incremental
  physical disk consumption. Cache/image overlap concerns the inventoried test
  images, not every reference elsewhere in the registry.
- [The smoke receipt](smoke-replacement.json) verifies and deletes exactly three
  representative replacement sandboxes: image indices 000/001/002, one per
  recipe, all `app-change-1`. It does not validate every produced image, the
  recent cohort, or the dependency-change variants in running sandboxes.

## Retention and cleanup boundary

The [final scoped prune](cache-prune-final.json) deleted 21 cache manifests,
reported 64 retained entries and 1,268,718,583 unique referenced bytes, and
removed no image records. This is a logical retention result; physical space
reclamation still depends on references and registry garbage collection.

Published test images and gateway contexts are retained as qualification
artifacts under their normal lifecycle; releasing builders or pruning cache
does not delete them. Temporary compute cleanup is tracked separately: the
three smoke sandboxes have deletion receipts, the preparation hold was
released, and the main run preserves stopped sampler evidence and verifies VM
retirement. A release receipt alone is not proof that every VM has retired.
