# Registry publication optimization

Deployed runtime commit `c09e2228197a41c755e13a1cceb9ad0714edaef1` at
2026-09-29 22:59:59 UTC after local tests, production byte-equivalence checks and
three 48-build load runs. Gateway and relay HTTPS checks passed after deployment.
All three application-image checks passed inside sandboxes on a freshly provisioned
worker, and the test sandboxes were deleted. Its installed 165-file package matched
the candidate wheel exactly; unattended-upgrade units were masked and inactive,
with automatic APT periodic settings disabled. All temporary workers retired
after validation. Cleanup removed all 145 owned test manifests and their 290
tags, preserving shared components, caches and blobs. The final production audit
verified installed files, future-node bundles, active services, healthy gateway
and relay HTTPS, zero database-pool waiters and no remaining test workload.

The preceding build qualification exposed Python dependency updates spending
33.8 seconds on average inside Docker pull during immutable filesystem
publication. That measurement includes downloading, decompression and filesystem
writes.

## Confirmed cause

Hash-verified inspection of an owned synthetic Python image found that its three
missing EROFS groups contain 345,718,795 compressed bytes and 1,109,656,576
unpacked tar bytes. Both exceeded the old selective-publication limits of
128 MiB compressed and 1 GiB unpacked. The 41,168 members had no unsupported
filesystem features, missing parents or duplicate paths. The fallback was
therefore avoidable for this workload.

The new aggregate limits are 512 MiB compressed and 2 GiB unpacked. Member,
filesystem-semantics and digest checks remain enforced. Production publication
admission remains two finishing builds per node, alongside four builds in
preparation/build/push.

## Changes

- Stream compressed registry bytes through hashing and decompression into the
  quarantine tar. This removes the compressed temporary-file write/read cycle.
  Both compressed and uncompressed identities must authenticate before extraction.
- Claim missing components in a consistent lock order before fetching their
  bytes. Recheck after waiting, allowing concurrent builders on the same node to
  reuse completed work. Claims release on success, fallback, cancellation and
  deadline expiry.
- Retain bounded fallback-reason counters and separate transfer, decompression
  and extraction measurements. Actual response-byte counts include failed
  attempts. Decompression timing excludes nested transfer wait but is not a
  CPU-only measurement.

No SDK changes or new client opt-in are required. The existing full Docker path
remains available for unsupported layers and inputs beyond the selective limits.

## Validation

The affected suite ran 170 tests in 6.640 seconds and passed, with two existing
platform skips. Tests cover corrupt and truncated streams, expansion limits, deadline and
response cleanup, real preparation subprocesses, multi-group concurrency,
component identities, fallback and diagnostic allowlists. Ruff and whitespace
checks passed.

The layer-inspection, multi-group comparison and builder-identity qualification
helpers passed 23 additional tests.

Post-deployment application checks exercised 1,500 Python modules, NumPy/SciPy,
pandas, a Parquet round trip, native C code and a small scikit-learn model. Both
TypeScript bundles ran 2,000 transforms with validation and aggregation. All
assertions passed. Sandbox creation took 88.3 seconds including provisioning a
new worker; this is not a steady-state latency measurement or a training soak.

A small local ABBA witness verified identical payloads while reducing quarantine
writes from 33,565,901 to 16,783,360 bytes. This demonstrates the removed disk
pass; it is not production throughput evidence.

An isolated production ABBA comparison forced all three missing groups. All four
passes reproduced the reference EROFS bytes and signed metadata exactly, including
618,655,744 bytes of rebuilt components. Both candidate passes used the preparation
subprocess and streamed 345,718,795 compressed bytes without a Docker pull. The
comparison blocked registry writes. Docker cache warming makes its individual
timings unsuitable as an end-to-end throughput comparison.

The load comparison kept the same four builders, 48 concurrent cases per arm,
four-plus-two admission policy and 600-second common arrival deadline. Each case
used a distinct dependency-build nonce; source contexts otherwise matched. The
baseline was repeated after the candidate to check the initial fresh-node cache
effect. All 144 builds succeeded with zero client deadline misses, and each wave
fully drained before the next operation.

| Measurement | Baseline A | Candidate B | Warm baseline A2 |
|---|---:|---:|---:|
| 48-build batch, seconds | 271.1 | 246.5 | 275.4 |
| Client p95, seconds | 234.8 | 220.5 | 260.8 |
| Python publication mean, seconds | 44.0 | 15.6 | 34.7 |
| Python Docker fallback count | 16 / 16 | 0 / 16 | 16 / 16 |
| Publication-slot waiting mean, all recipes, seconds | 4.89 | 0.94 | 1.78 |

Against the warm repeat, the candidate batch finished 10.5% sooner and Python
publication took 55.0% less time. All 16 candidate Python builds used selective
publication. Across all recipes, 40 candidate builds materialized selected
layers and eight reused complete filesystem components; none pulled through
Docker. The improvement was not uniform across recipes: TypeScript tools
publication averaged 14.2 seconds in B versus 12.7 seconds in A and 12.0 seconds
in A2.

The largest remaining Python phase is build-and-push, approximately 96 seconds
per build in both A and B. Admission waiting also contributes to batch latency;
the registry change does not remove dependency installation or compilation.

Across the four builders, measured disk writes fell to 56.6 GiB in B versus
67.7 GiB in A2 (80.4 GiB in A). This is aggregate device traffic during each
wave, not unique filesystem growth. Gateway registry-volume writes remained
approximately 14.2 GiB in all three waves, with I/O-weighted wait near 47–48 ms;
publication still has a substantial registry write cost.

Candidate builders used 5.9–6.5 of their eight CPU cores in each host's busiest
30-second CPU window. Whole-gateway CPU averaged 0.42 cores during B, including
the local benchmark driver; the API process averaged 0.075 cores. This build
test alone is insufficient to size the gateway for live sandbox traffic.

All 70 sampled gateway-local HTTPS health checks succeeded. Candidate health
latency p95 was 11.9 ms across 21 checks. No host recorded OOM kills or swap I/O
during the three measured waves; candidate builders retained at least 24.3 GiB
of available memory. Telemetry covered 98.8–99.6% of each gateway measurement
window, with an additional 60-second tail before subsequent operations.

These are three ordered synthetic build waves, not repeated randomized trials.
Base images, package downloads and shared caches can remain warm even though each
dependency RUN is invalidated. The existing autoscaler also warms an idle sandbox
worker during image builds. This qualification does not establish capacity for
500–1,000 concurrent running sandboxes or guarantee arbitrary training deadlines.
Raw production telemetry and operational receipts remain outside this public report.
