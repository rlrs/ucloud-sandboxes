# Selective preparation execution model — 2026-09-29

This follow-up release was deployed at **12:09:34 UTC** with healthy gateway,
relay and metrics checks. It moves authenticated
selective extraction and private-diff squashing into a bounded child process,
avoiding contention between publications sharing one Python interpreter. An
all-cache-hit publication avoids child startup. Deployment, runtime
identity, a representative 48-build run, application smokes and final cleanup
are separate gates. **All 48 representative builds passed in 115.495 seconds,
versus 139.054 seconds for R1: 16.9% faster in this historical comparison.**
Environment-publication p95 fell from 9.789 to 3.738 seconds. Deployment, exact
EROFS equivalence, load, all three application smokes and the combined final
cleanup audit passed. Placement/cache chronology and two complete component-cache
hits prevent attributing the whole batch improvement solely to process isolation.

[Source verification](candidate-source.json) records all **163 packaged files**
matching candidate wheel SHA-256
`b1580ae4ffc8e81a81cb5cc56f4e539bb084a07c2a16bbb95f57c6707448917d`.
The runtime change is committed as `0371094`.
The baseline is the deployed R1 wheel
`7b00954ae39cae9c552c238769dea193df0f5388b4ef8de6155670aeba5a947a`.
[Staging](staging-receipt.json) completed at 12:07:48 UTC with unchanged dependency
and native inventories for both future-node bundles. The
[deployment receipt](deployment-receipt.json) records activation; only
`node_package_root` changed. The paired gateway/configuration rollback is under
`/work/ucloud-sandboxes/builder-execution-20260929-r1/rollback`; use this release's
[controller](deployment-controller.py) for rollback.

## Evidence and scope

The preceding [filesystem preparation release](../builder-preparation-2026-09-29/README.md)
passed 48/48 builds and three application smokes. Its environment-publication p95
improved from 12.288 to 9.789 seconds, but batch completion was 139.054 seconds
versus 135.561 seconds previously, so it did not establish an overall speedup.

[The Linux root diagnostic](../builder-preparation-2026-09-29/PROCESS-COMPARISON.md)
measured the same R1 candidate using four threads and four fresh spawned
interpreters. Mean preparation took 7.439 seconds with threads and 1.768 seconds
with processes; fresh-process startup raised the latter to 2.001 seconds.
All 16 outputs matched and privileged filesystem probes had no skips. This is a
synthetic filesystem result, excluding registry transfer, BuildKit, EROFS
creation and signing. It supports an implementation trial, not a full-build
speedup claim or a claim that the interpreter lock alone caused the difference.

The implementation retains authenticated input handling, unsupported-layer
fallback, private scratch ownership, group-lock rechecks, EROFS identity,
signing and publication. Children have bounded lifetimes and are terminated and
reaped on timeout/failure. Existing build admission bounds their count.
Numeric child timings remain distinct from parent publication time.
The implementation records `selective_subprocess_ms` for startup, extraction,
squash and IPC; `selective_materialization_ms` and `squash_ms` remain nested child
stage timings. They must not be added to the enclosing subprocess duration.

[The real tools-image canary](tools-erofs-equivalence.json) passed four ABBA
trials against the pinned existing OCI image. Every trial reproduced all five
published signed components and rehashed EROFS bytes; the forced-missing final
component was 14,925,824 bytes. Both candidate trials reported positive child
timings (1,666.518 and 1,666.188 ms), confirming that the isolated path ran.
Registry writes were zero because publication output was captured for comparison.
These trials establish correctness of this input, not representative build
latency; the first Docker arm warmed its local cache.

## Representative qualification — completed

- Release root: `/work/ucloud-sandboxes/builder-execution-20260929-r1`.
- Load root: `/work/ucloud-sandboxes/builder-execution-load-20260929`.
- Phase: `exec-repeat`; managed image prefix: `bl20260929-exec-repeat-`.
- Reservation: `builder-execution-20260929`.
- Application receipt: `smoke-exec-repeat.json`.
- Gateway sampler: `ucloud-execution-load-monitor`, using public `/healthz`.

The R1 builder reservation was released, and this qualification uses four fresh
R2 builders: `167960902`, `167960903`, `167960906` and `167960907`. Their retained
`builder-*-initial.json` receipts confirm active node agents, all four candidate
module fingerprints and initially empty local BuildKit caches. The retained
[comparison](qualification-comparison.md) confirms the same 48 frozen context
hashes as R1 `prep-repeat`, with complete client timings and 48 durable-history
lookups. Local and registry cache history and node placement remain comparison
confounders. Do not reinterpret R1's
23 HTTP401 sampler responses from its mistaken `/health` URL as successful
health probes. R1's passed application smokes and pre/post `/healthz` checks are
independent evidence.

| Measurement | R1 `prep-repeat` | R2 `exec-repeat` |
| --- | ---: | ---: |
| Successful builds | 48/48 | 48/48 |
| Batch completion | 139.054 s | 115.495 s |
| Client median / p95 | 87.754 / 136.559 s | 69.718 / 112.104 s |
| Submission median / p95 | 41.709 / 98.797 s | 32.275 / 79.793 s |
| Build-and-push median / p95 | 34.215 / 50.801 s | 27.074 / 44.012 s |
| Environment publication median / p95 | 3.021 / 9.789 s | 1.805 / 3.738 s |
| Submit 503 responses / repeated attempts | 775 / 775 | 560 / 560 |
| Maximum admitted / executing per builder | 4 / 4 | 4 / 4 |
| Node queue p95 | 15 ms | 5 ms |
| All-group cache hits avoiding child work | 0 | 2 |
| Physical registry writes, client window | 0.628 GiB | 0.580 GiB |
| Successful public health probes in window | Invalid `/health` probe | 19/19 `/healthz` |
| Application smokes passed/deleted | 3/3 | 3/3 |
| Combined provider/reservation/final health audit | Passed at 12:24:13 UTC | Passed at 12:24:13 UTC |

The fleet reached 16 admitted/executing builds. Submission includes admission
retries and context transfer; HTTP status counts do not identify individual
response error codes. No duplicated build IDs or terminal errors were observed.
All 48 skipped a full Docker pull. There were 46 child materializations and two
complete signed-component-cache hits: Python case `006` and tools case `034`.
Those hits correctly have no child timer. Overall, 162 groups were reused and
46 built, from 92,809,909 selected compressed OCI bytes. Compared with R1's 48
misses, this is another reason not to treat the entire latency delta as an
isolated execution-model effect.

[All three application smokes](smoke-exec-repeat.json) executed successfully and
were deleted by 12:19:20 UTC, one image from each measured recipe. The builder
reservation was [released at 12:21:12 UTC](pool-release.json), with no pending
build or preparation demand. Normal node retirement and the final combined audit
subsequently passed at 12:24:13 UTC.

### Preparation timing and fixed overhead

[The phase-cost report](phase-costs.md) contains numeric coverage and per-recipe
distributions. Across the 46 child invocations, subprocess median/p95/maximum
was **0.766 / 2.213 / 2.320 seconds**. Nested selective materialization p95 fell
from 4.929 seconds over R1's 48 misses to 1.144 seconds over R2's 46; squash p95
fell from 2.961 to 0.708 seconds. Their combined per-build p95 fell from 7.895 to
1.830 seconds. Child startup/imports/IPC and other residual work cost median
336 ms and p95 393 ms, calculated before aggregation from each child's timers.
No child cost is assigned to the two complete cache hits.

The benefit is uneven for small outputs:

| Recipe: environment median / p95 | R1 | R2 |
| --- | ---: | ---: |
| Python agent | 3.049 / 4.495 s | 1.845 / 2.098 s |
| TypeScript tools | 4.813 / 9.975 s | 3.494 / 3.955 s |
| TypeScript multistage | 0.705 / 0.881 s | 0.783 / 0.963 s |

The smallest multistage output became modestly slower while larger preparation
tails improved. This is consistent with fixed invocation overhead being more
visible for tiny work, but the historical comparison does not isolate that cause.
Build-and-push remains the largest phase; these changes do not eliminate
application lint, compilation and test execution. Cache preparation/mount p95
was 6/462 ms with 48/48 coverage, already included in build-and-push.

### Host resources

[Host findings](host-findings.md) cover 98.3% of the client window on every host.
Gateway CPU averaged 0.429 occupied cores (p95 1.730), including API 0.066,
registry 0.122 and the gateway-local SDK driver 0.169. All 19 public `/healthz`
probes succeeded, with p95 12.95 ms. These gateway-origin probes and a local SDK
driver do not establish external end-to-end gateway capacity.

The registry volume wrote 0.580 GiB during covered client intervals, with
I/O-weighted await 3.10 ms and I/O PSI mean/p95 8.70%/36.78%. Physical writes and
the earlier registry pre-mount benefit are distinct from extraction/squash
latency. Disk busy counters are excluded from capacity conclusions. Samples
extend about one minute beyond completion for separate writeback analysis.

Builder mean CPU was 3.084–4.225 of eight cores, p95 6.605–6.815 cores; CPU PSI
mean was 7.09–9.83%. Available memory stayed at least 25.28 GiB on builders and
13.14 GiB on the gateway. No OOM kills or swap activity were observed in covered
intervals. These batch averages do not justify increasing all concurrency limits.

### Registry reuse and pruning

[Registry aggregation](candidate-registry.json) recorded 840 successful mounts
and no declined mount responses, with 64,368,328 managed-image committed blob
bytes and 64,139,613 cache committed blob bytes. These completed HTTP events use
immutable blob sizes, not measured request bodies or physical writes. The earlier
pre-mount optimization remains effective; the smaller totals also reflect this
run's reused outputs and cache history.

[Normal cache pruning](cache-prune.json) deleted 48 of 112 inventoried cache
manifests and retained 64 entries referencing 953,222,353 bytes. It did not run
global garbage collection; manifest deletion does not establish physical byte
reclamation. [The owned-image inventory](image-inventory.json) records sharing
across the 48 measured images:

| Descriptor accounting, decimal GB | Summed across images | Unique content |
| --- | ---: | ---: |
| Compressed OCI layers | 19.410 | 0.928 |
| Signed EROFS image bytes | 32.771 | 1.854 |

The retained shared cache references 0.953 GB of blobs, of which 0.928 GB also
belongs to these test images. These are different logical accounting views;
they are not unpacked filesystem size or measured registry disk usage. Metadata,
in-progress uploads and pending garbage collection are excluded.

[The execution analyzer](analyze-execution.py) reports each numeric timing's
coverage, median, p95 and maximum, including `selective_subprocess_ms`. It also
calculates each record's subprocess duration minus its extraction and squash
durations before aggregating residual overhead. The residual includes imports,
IPC and uninstrumented work; it is not a pure process-startup measurement. Missing
R1 subprocess fields remain unreported, never zero.

Reproduce from the retained R2 summary and telemetry:

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/builder-execution-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/builder-execution-2026-09-29
python3 docs/benchmarks/build-optimization-2026-09-29/analyze-qualification.py \
  docs/benchmarks/builder-execution-2026-09-29/exec-repeat/summary.json \
  --baseline docs/benchmarks/builder-preparation-2026-09-29/prep-repeat/summary.json \
  --output-prefix docs/benchmarks/builder-execution-2026-09-29/qualification-comparison
python3 docs/benchmarks/builder-execution-2026-09-29/analyze-execution.py
```

## Combined final audit

[The completed receipt](final-state.json) reports `audit_complete: true` at
**12:24:13 UTC**: 96/96 exact successful history identities, all six application
smokes verified/deleted, all 11 temporary nodes retired, no fleet nodes,
sandboxes, active builds or reservations, and both samplers stopped. Gateway and
relay TLS/HTTPS, metrics and idle lifecycle checks passed with the 64 GiB relay
budget preserved. All 163 installed packaged files and both future-node bundle
hashes matched the qualified R2 release. The 96 measured managed image aliases
and local qualification artifacts are intentionally retained.

[The publication artifact audit](artifact-audit.json) inventories both release
directories with file hashes and a bounded credential-pattern/JSON-schema review.
It found no embedded credentials or raw customer logs; authorized operational
IDs, numeric metrics and owned synthetic fixture metadata remain present. This
is an artifact review, not a repository-wide security audit.

[The audit script](final-audit.py) is read-only apart from exclusively creating
a new mode-0600 receipt. It uses the gateway's existing SDK/provider credentials
without copying or printing them. It requires:

- Both phases' 48 unique build UUIDs and image IDs, with exact read-only SQLite
  history lookups for all 96 identities.
- Three successful, verified and deleted application sandboxes per phase, tied
  to three distinct images from that phase with matching image/recipe identities.
- R2 wheel digest, every packaged installed file, both node-bundle digests,
  staging/deployment receipt consistency and the configured future-node root.
- The R2 controller's authenticated loopback and public HTTPS `/healthz` checks,
  healthy metrics, the 64 GiB relay budget and no outstanding relay/lifecycle work.
- No sandboxes, active builds, fleet nodes, pending capacity or preparation
  reservations; both known gateway samplers stopped. Fleet count uses a direct
  read-only SQLite heartbeat query and does not initialize the control store.
- Retirement of the union of nodes in both phase receipts and explicit known
  profiling/build/smoke nodes `167955324`, `167957685`, `167957686`, `167957690`,
  `167957691`, and `167958242`. Add newly created owned nodes with `--known-node`.
  This run's application worker `167961272` must be included explicitly; R2
  builder IDs are also collected from the phase receipts.

Stage the script on the gateway only after review, then use the current gateway
venv. Substitute the exact qualified R2 wheel SHA-256:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/builder-execution-load-20260929/final-audit.py \
  --wheel-sha256 b1580ae4ffc8e81a81cb5cc56f4e539bb084a07c2a16bbb95f57c6707448917d \
  --known-node 167961272 \
  --output /work/ucloud-sandboxes/builder-execution-load-20260929/final-state.json
```

The command above identifies the completed audit's inputs; choose a new output
path for any repeat so the original receipt remains unchanged. The R1 audit
remains fixed to its original phase. Running it with
a different output path would still audit R1. This combined audit reads both
original phase roots and preserves their earlier receipts. It must fail its
cleanup gate if test builders or reservations remain; it does not release
reservations, stop services or delete resources. Run it after authorized cleanup,
as done for the receipt linked from both releases. Local synthetic validation uses
`python3 docs/benchmarks/builder-execution-2026-09-29/final-audit.py --self-test`
and performs no network or production calls.

The build workload and final idle audit do not qualify 500 or 1,000 concurrently
running agent sandboxes or establish mixed-workload gateway capacity.
