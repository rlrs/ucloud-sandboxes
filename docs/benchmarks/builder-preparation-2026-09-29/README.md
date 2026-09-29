# Builder filesystem preparation optimization — 2026-09-29

Deployed at 11:39:31 UTC with healthy gateway, relay, and metrics checks. The
candidate removes redundant work while extracting and squashing privately owned
OCI diffs. On a Linux builder, four concurrent preparations completed their
timed stages in **11.489 seconds versus 19.815 seconds**, a **42.0% reduction**,
with equivalent filesystem outputs and all privileged semantic probes exercised.

**All 48 production builds passed.** Environment-publication p95 improved from
12.288 to 9.789 seconds in the historical comparison, but batch completion was
139.054 seconds versus 135.561 seconds previously. This run therefore does not
show an end-to-end build latency improvement. All three application smoke
sandboxes passed and were deleted. The builder reservation was released, and
[the combined final audit](../builder-execution-2026-09-29/final-state.json)
passed at 12:24:13 UTC, verifying both releases' cleanup and exact build history.

## Change and baseline

The preceding [registry-I/O release](../registry-io-2026-09-29/README.md) reduced
registry writes but exposed local extraction and squashing as important parts
of the remaining environment-publication tail. This release changes four
runtime modules:

- OCI extraction reuses validated path information and uses bounded kernel
  copies for regular-file payloads from already authenticated tar archives,
  with a tested userspace-copy fallback. Digest, path, type, size, and metadata
  validation remain enforced.
- Squashing can consume disposable private extraction trees, moving regular
  files and symlinks instead of copying their payloads and metadata again.
  The selective materializer explicitly opts into this path. Borrowed Docker
  diffs retain the existing copy contract; lower layers remain read-only.
  Cross-filesystem moves fall back to copying.
- Build execution records `cache_prepare_ms` and `cache_mount_ms`, and durable
  history retains them. These are nested within the build/build-and-push
  phase, not additional elapsed time to add to it. This closes the previous
  run's observability gap when early cache-mount log lines were truncated.

The baseline source is commit
`48f30bd1bbc3e74fbf16576c8b7964db60b2817a`, deployed in wheel
`59229802be015443b0dd61f2c0c1b8a1eb9cd4460fc35eaf003a601c72b7009b`.
The candidate wheel is
`7b00954ae39cae9c552c238769dea193df0f5388b4ef8de6155670aeba5a947a`.
[Source verification](candidate-source.json) confirms that all 162 packaged
source/resource files match the candidate and records the four changed module
hashes. EROFS layout, compression, signing, and component identities are not
intentionally changed.

## Linux preparation qualification

[The retained Linux result](linux-four-thread.json) compares baseline and
candidate in ABBA order on the same temporary builder, with Python 3.14.4,
Linux 7.0.0-30, and UID 0. Each arm starts a fresh process and runs four threads
against fresh private output trees. The fixture contains 2,000 application
modules and 2,000 small-file `node_modules` packages across four OCI layers.
The candidate enables private-diff consumption; the baseline copies.

| Mean over the two runs per arm | Baseline | Candidate | Reduction |
| --- | ---: | ---: | ---: |
| Four-publication extraction wall time | 8.425 s | 5.271 s | 37.4% |
| Four-publication squash wall time | 11.389 s | 6.218 s | 45.4% |
| Sum of timed stage wall times | 19.815 s | 11.489 s | 42.0% |
| Sum of timed stage process CPU | 33.562 CPU-s | 19.815 CPU-s | 41.0% |

All 16 publication outputs matched under the benchmark's comparison rules, and
source fingerprints remained stable within each arm. User xattrs, real deletion
whiteouts, lower-layer whiteouts, and trusted directory opacity all ran with
**no capability skips**. Contents, object types/modes, owners, modification times,
symlink targets, xattrs, and hardlink relationships are compared. Generated
whiteout modification times are normalized because the existing squasher assigns
execution time; atime, ctime, and numerical inode identities are not compared.

The [reproduction protocol](MICROBENCHMARK.md) explains the stage barriers,
warm fixture bytes, fresh destinations, profiling separation, and local results.
Validation runs outside timed stages. This synthetic fixture is highly
compressible and excludes network, registry, Docker, BuildKit execution,
`mkfs.erofs`, signing, and production overlap between stages. Its improvement
must not be presented as a 42% reduction in complete image-build latency.

## Exact EROFS and root semantic gates

[The real tools-image ABBA canary](tools-erofs-equivalence.json) used a pinned
existing OCI manifest, forced only the final component to be treated as missing,
and compared the existing Docker extraction path with the candidate selective
path. All four trials reproduced all five published components, including the
same signed metadata and rehashed EROFS bytes. The newly rebuilt component was
14,925,824 bytes, with image digest
`sha256:17092f638536f932bd5b1023f8fa537677812ece4349e986130b6d56ee3cd965`.
The format remained layout 1, LZ4, and `mkfs.erofs` 1.9. Registry writes were zero:
publication output was captured locally for verification. The first Docker arm
warmed its local cache, so these four canary timings are correctness evidence,
not a fair end-to-end speed comparison.

[The root-only read-only-directory canary](readonly-diff-canary.json), whose
[source is retained](readonly-diff-canary.py), also passed. It compares copied
and consumed trees containing read-only directories, non-root uid/gid ownership,
setuid modes, directory/file xattrs, hardlinks, overwritten linked files, and
relative, absolute, and dangling symlinks. It verifies that borrowed source trees
remain unchanged and that the consumed regular file retains its inode while
the payload-copy helper is forbidden. All canary temporary files are removed
before success is reported.

[Focused test verification](test-verification.md) records the exact 39-, 100-,
and 42-test commands, overlapping scopes, and platform skips. Those counts are
not summed. Linux live canary evidence is separate from tests skipped locally.

## Deployment and rollback

The [staging receipt](staging-receipt.json) pins the builder and sandbox bundles
and confirms unchanged native and dependency inventories. The
[deployment receipt](deployment-receipt.json) records activation and health.
The release controller changes only `node_package_root` and retains paired
gateway-venv/configuration rollback under
`/work/ucloud-sandboxes/builder-preparation-20260929-r1/rollback`.
Use [this release's controller](deployment-controller.py) for rollback rather
than an earlier release's backup. The temporary profiling reservation was
[released](profile-pool-release.json); provider retirement remains part of the
final audit.

## Full production build qualification

The completed `prep-repeat` qualification used four fresh candidate builders and
the same 48 frozen contexts as the preceding `io-repeat` baseline: 16 revisions
each of Python scientific/native-extension, TypeScript tools, and TypeScript
multistage images. [The comparison](qualification-comparison.md) verifies the
same context-hash set and includes all 48 terminal outcomes and full SDK
submission-to-completion time, including admission retries. Placement, registry/cache history,
and fresh hosts can differ; one historical burst comparison is not a randomized
latency experiment.

| Measurement | Previous `io-repeat` | Candidate `prep-repeat` |
| --- | ---: | ---: |
| Successful builds | 48/48 | 48/48 |
| Batch completion | 135.561 s | 139.054 s |
| Client median / p95 | 76.800 / 132.432 s | 87.754 / 136.559 s |
| Build-and-push median / p95 | 31.155 / 57.641 s | 34.215 / 50.801 s |
| Environment-publication median / p95 | 4.196 / 12.288 s | 3.021 / 9.789 s |
| Retained cache preparation/mount timing | Unavailable | 48/48 records for each |
| Physical registry writes, client window | 0.651 GiB | 0.628 GiB |
| Three application smoke sandboxes passed/deleted | 3/3 | 3/3 |
| Durable history lookups reported by load harness | 48/48 | 48/48 |
| Provider, reservations, sampler, and final health audit | Passed | Passed in combined R1/R2 audit |

The fleet reached 16 admitted/executing builds, at most four per builder.
Node queue p95 was 15 ms; submission p95 was 98.797 seconds and included admission
retries. The harness observed 775 HTTP 503 submit responses and 775 repeated
submit attempts, versus 711 previously, with no duplicate build identities or
terminal failures. These status counts do not identify individual response error
codes. The telemetry cannot attribute the longer client wait to a particular
resource; raising concurrency is not justified by the gateway's low mean CPU.
All 48 builds used selective materialization and skipped a full Docker pull,
reusing 160 groups and building 48 new groups from 96,993,279 selected OCI bytes.

[The application smoke receipt](smoke-prep-repeat.json) covers one measured image
from each recipe. All three executed successfully and were deleted by 11:51:13
UTC. [Registry request aggregation](candidate-registry.json) records 852
successful cross-repository mounts, no declined mount responses, and 68,382,405
managed-image committed blob bytes, close to the preceding run's 68,380,565 bytes.
These are completed HTTP events joined to immutable blob sizes, not physical
write savings or upload-body measurements. The separate physical-write counters
remain the relevant storage evidence.

[Cache pruning](cache-prune.json) removed 48 cache manifests from 112 inventoried
tags, retaining 64 entries and 960,211,467 referenced bytes under the existing
policy. Manifest deletion does not establish physical byte reclamation; normal
registry garbage collection remains responsible for unreferenced blobs. The
candidate builder reservation has since been [released](pool-release.json),
and the subsequent qualification used fresh builders. Final provider retirement
was verified separately; the pruning receipt does not claim fleet cleanup.

### Retained preparation costs

[The phase-cost report](phase-costs.md) retains complete 48/48 timing coverage.
Cache preparation took median **3 ms**, p95 **7 ms**, maximum **8 ms**. The bounded
cache pre-mount pass took median **290.5 ms**, p95 **677.1 ms**, maximum
**1,095 ms**. These costs are already included in build-and-push, and the previous
release did not retain them as numeric fields.

| Environment subphase, median / p95 | Previous | Candidate |
| --- | ---: | ---: |
| Selective materialization | 1.335 / 4.800 s | 1.061 / 4.929 s |
| Squash | 1.506 / 5.719 s | 0.548 / 2.961 s |
| Per-build materialization + squash | 2.841 / 10.555 s | 1.665 / 7.895 s |
| EROFS creation | 0.115 / 0.434 s | 0.129 / 0.499 s |
| Component publication | 0.176 / 0.484 s | 0.238 / 0.574 s |

The largest observed reduction is squashing. Selective materialization includes
download, authentication, decompression, validation and extraction, so its p95
does not isolate the extractor change. The TypeScript-tools recipe still had a
5.014-second materialization p95 and 3.134-second squash p95. Cache mounts,
component lookup (p95 2.978 ms), and group-lock wait (p95 0.120 ms) are much smaller
than the remaining BuildKit execution and preparation tails. Parent and child
timers overlap; do not add preflight to its measured subphases or add independent
percentiles. The combined materialization/squash row sums each build first.

### Host resources and health-probe limitation

[Host findings](host-findings.md) use complete sampled intervals within the
client window, with 97.8% gateway and 97.8–99.3% builder coverage. Gateway CPU
averaged **0.360 occupied cores**: API 0.069, registry 0.104, and the gateway-local
SDK driver 0.115. Registry-volume writes were 0.628 GiB, with I/O-weighted await
3.65 ms and I/O PSI mean/p95 9.61%/29.60%. Disk busy counters are excluded from
capacity conclusions. The small write-volume difference from the previous run
does not establish a further registry-I/O improvement.

Builder mean CPU ranged from 2.753 to 3.879 of eight cores, with sampled p95
6.265–6.795 cores. CPU PSI mean was 7.01–9.95%; at least 24.52 GiB of memory was
available. No OOM kills or swapping were observed in covered intervals. These
batch averages include quiet submission and completion periods and do not prove
headroom during the busiest execution windows.

**The in-burst sampler health gate was invalid.** Its configured URL was
`https://77.42.92.27/health`, which returned HTTP 401 in all 23 in-window probes;
the intended public endpoint is `/healthz`. Raw failed-probe counts are preserved
in generated reports. They establish neither a TLS failure/service outage nor
successful health during the burst. The 1,811 SDK HTTP 200 responses, 48 accepted
builds and 48 terminal successes are independent functional evidence. The
deployment controller correctly checks `/healthz`; final health and the passed
application smoke receipts remain separate gates. Future samplers must use `/healthz`.

### Execution-model follow-up

[The completed Linux thread/process diagnostic](PROCESS-COMPARISON.md) compares
the same deployed candidate source on the same root-capable builder. Four fresh
processes took 2.001 seconds including startup and preparation, versus 7.439
seconds for four threads' preparation alone; all 16 outputs and privileged
semantic probes passed. This is isolated filesystem evidence, not a full-build
speedup. It motivates a separately qualified child process only for selective
cache misses, leaving all-cache-hit publication in the parent. The
[next release's record](../builder-execution-2026-09-29/README.md) records its
deployment, representative load and passed combined final cleanup gates.

### Reproduce and final cleanup

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/builder-preparation-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/builder-preparation-2026-09-29
python3 docs/benchmarks/build-optimization-2026-09-29/analyze-qualification.py \
  docs/benchmarks/builder-preparation-2026-09-29/prep-repeat/summary.json \
  --baseline docs/benchmarks/registry-io-2026-09-29/io-repeat/summary.json \
  --output-prefix docs/benchmarks/builder-preparation-2026-09-29/qualification-comparison
python3 docs/benchmarks/builder-preparation-2026-09-29/analyze-preparation.py
```

[The completed combined audit](../builder-execution-2026-09-29/final-state.json)
verified all 96 exact successful build identities across R1/R2 and all six
successful/deleted smoke sandboxes. All 11 known temporary nodes, including
profiling VM `167955324`, were absent from provider inventory. Fleet, sandbox,
active-build and reservation counts were zero; both samplers were stopped and
gateway/relay HTTPS, metrics and idle checks passed.

The original [R1 audit script](final-audit.py) is retained for provenance and was
superseded by the [combined R2 audit](../builder-execution-2026-09-29/final-audit.py),
which performs a direct read-only heartbeat count and binds each smoke recipe
to its distinct measured image. Use the combined audit for repeat verification,
with a new output path. Final idle health does not repair the invalid in-burst R1
health probe. This build test does not qualify 500 or 1,000 concurrently running
agent sandboxes.
