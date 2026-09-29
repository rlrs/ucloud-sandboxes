# Exact-cache single-import qualification — 2026-09-29

This release offers only the newest exact recipe/input cache when one exists.
A miss keeps the existing bounded eight-import fallback. The fresh-builder
repeat completed **48/48 builds in 54.656 seconds**, versus 98.594 seconds with
eight imports, an observed **44.56% reduction**. Client p95 fell from 97.275 to
21.683 seconds. These are results for the same 48 frozen contexts, not a
guarantee for uncached builds or every workload.

It was [deployed](deployment-receipt.json) at **14:00:19 UTC**, with healthy
gateway, relay and metrics checks. [Three application smokes](smoke-single-import-repeat.json)
passed and their sandboxes were deleted. The [owned reservation was released](pool-release.json)
at 14:08:47 UTC. The [final audit passed](final-state.json) at **14:13:56 UTC**:
all five owned nodes were absent from the provider, all 48 durable histories
matched, the gateway was healthy and idle, and driver/sampler units were stopped.
[Candidate source verification](candidate-source.json)
records all 163 packaged files matching wheel SHA-256
`33c89ef609c5f7fc014d0a8e1c1e7b698566b524cbba46cebcc73d3a90bb76df`
at runtime commit `6f8a569`. [Staging](staging-receipt.json) preserved dependencies
and native files in both future-node bundles. Helper preparation and local
self-tests made no production calls.

The preceding [affinity qualification](../build-cache-affinity-2026-09-29/README.md)
is independently auditable. Its eight-import repeat completed 48/48 builds in
98.594 seconds, but some expensive application steps still executed. Its
[controlled diagnostics](../build-cache-affinity-2026-09-29/cache-diagnostics.md)
support fewer imports; their `cacheonly` times are not image-build batch times.
This measured phase retains image push and EROFS publication.

## Measured result

The [comparison](performance-comparison.md) verifies all 48 matching
recipe/variant/context identities. The candidate ran from **14:04:42 to
14:05:37 UTC**, on four fresh builders whose initial receipts show empty local
BuildKit caches and the installed candidate source. SDK 0.4.33, fixture bytes,
base pins, dependency locks, BuildKit version, four slots per builder and the
48-request harness remained fixed.

| Measurement | Previous eight-import repeat | Single exact import |
| --- | ---: | ---: |
| Successful builds | 48/48 | 48/48 |
| Batch wall seconds | 98.594 | 54.656 |
| Client p95 seconds | 97.275 | 21.683 |
| Submission p95 seconds | 65.685 | 19.522 |
| Build/push median seconds | 21.647 | 1.318 |
| Build/push p95 seconds | 50.656 | 2.355 |
| Environment publication p95 seconds | 4.249 | 0.940 |
| Submit HTTP 503 observations | 309 | 55 |

[Selection evidence](single-import-repeat-selection.json) covers all 48 logs:
all selected an exact affinity match and **one import**. All 48 exported one
immutable cache manifest digest. Forty-three can be joined to a unique cache
tag; five have digest aliases, retained explicitly in the report. Selection is
separate from actual BuildKit execution evidence. In the retained
[progress](candidate-buildkit-progress.json), only one request has an executed
application vertex, versus 28 such observations in the previous repeat.
Progress can contain shared/replayed vertices; these counts are not independent
CPU executions or a proof about omitted log lines.

**The remaining tail is real.** Case 22, `typescript-tools/app-change-12`, took
54.637 seconds end to end: submission was 17.339 seconds, build/push 33.629
seconds and environment publication 2.880 seconds. Its progress includes
8.6 seconds of cached-layer materialization, 3.6 seconds of source copy and a
19.9-second application `RUN`; image export was 1.0 second and cache export
0.2 seconds. These nested observations describe the slow path without proving
why that selected cache did not avoid the application work. The batch maximum
must not be replaced by the much lower client p95.

The [outlier investigation](case22-outlier.md) verifies a complete imported tools
cache and overlapping same-variant multistage work on the same BuildKit daemon.
The shared-graph interaction remains a plausible explanation; the run did not
trace the internal cache-manager decision. The other 47 requests finished by
21.788 seconds. This remaining miss warrants a bounded reproduction before
changing production daemon topology or export policy.

The [build report](build-load-report.md) records **207 EROFS groups reused and
one built**, and no full Docker pull for any of the 48 builds. The built group
contains 14,925,824 EROFS bytes; that is not total image size or physical registry
growth. Admission remains visible: submission p95 is 19.522 seconds, while
worker queue p95 after admission is 0.006 seconds. The 55 retried submit 503s are
admission observations; all requests ultimately completed successfully.

The sequential comparison retains registry cache history, host cache, placement
and scheduling differences. The earlier controlled source/ARG invalidation
proof and fan-in diagnostics support the change, but do not establish a universal
BuildKit root cause or promise that an uncached compile becomes faster.

## Host evidence and validation

[Host findings](host-findings.md) cover 98.2% of the gateway phase and
94.5–98.2% of the builder phases. The gateway's occupied CPU cores were
**0.875 mean / 3.075 p95 / 3.135 maximum**. Mean process attribution was
0.040 cores for the API, 0.322 for the registry and **0.325 for the SDK driver**.
The driver ran under the required service name, so its sampled attribution is
available in this run; it was unavailable in the SSH-launched baseline. The
shorter burst concentrates early work, and independent p95 values cannot be
subtracted to derive headroom.

The registry volume wrote **0.198 GiB within the measured window**, with
I/O-weighted await 3.57 ms. Gateway I/O PSI was 9.87% mean / 46.61% p95 and
iowait 0.250 / 1.300 cores, so the low total write count does not imply no
storage stalls. Disk busy counters are excluded from conclusions because of
known accounting anomalies. Samplers retained more than 60 seconds of tail
after the burst; the quoted volume bytes use the phase window only.

Builders averaged 0.269–1.389 occupied cores, with sampled maxima no higher
than 2.720, and had at least 27.47 GiB available memory. Gateway available
memory stayed above 13.04 GiB. No OOM delta or swap activity was observed in
covered intervals. All **five gateway `/healthz` probes passed**, p95 14.27 ms;
these gateway-local probes are not external end-to-end capacity measurements.
This is an image-build qualification, not a 500/1,000-running-agent test.

[Verification](test-verification.md) records 80 passing focused tests, helper
self-tests and independent reviews. All three recipe-bound application smokes
verified output, exited zero and were deleted. [Normal cache maintenance](cache-prune.json)
retained 64 entries totaling 928,227,640 referenced bytes and removed 48 cache
manifests; physical reclamation still follows registry GC policy. The
[48-image inventory](image-inventory.json) finds 927,947,641 unique compressed
OCI-layer bytes shared with the cache and only 279,999 additional cache-config
bytes in this metadata view. Referenced descriptors exclude filesystem metadata,
pending uploads and unreclaimed blobs; they are not physical volume consumption.
The
[resource ledger](resource-ledger.json) confirms retirement of the four builders
and smoke worker and binds the completed final-audit receipt. The bounded
[publication review](artifact-audit.json) covers these structured receipts,
telemetry, reports and helpers; it found no credential-pattern matches or raw
customer workload payloads.

| Item | Fixed value |
| --- | --- |
| Release root | `/work/ucloud-sandboxes/build-cache-single-import-20260929-r1` |
| Load root | `/work/ucloud-sandboxes/build-cache-single-import-load-20260929` |
| Phase | `single-import-repeat` |
| Image prefix | `bl20260929-single-import-repeat-` |
| Reservation | `build-cache-single-import-20260929` |
| SDK driver unit | `ucloud-build-load-client-single-import-repeat` |
| Gateway sampler unit | `ucloud-single-import-load-monitor` |
| Smoke receipt | `smoke-single-import-repeat.json` |
| Workload | 48 frozen contexts, three recipes × `app-change-5` through `app-change-20` |
| Builders | Four fresh builders; four admitted builds and four BuildKit solvers per builder |
| Cache export | Existing `mode=min`, compression, retention and pinned BuildKit 0.33.0 |
| SDK | Frozen 0.4.33; no archive or admission-protocol changes |

There was no new seed phase: the run reused the retained registry cache inventory.
Replacing the builder pool prevents local BuildKit cache from hiding the effect.
Record cache inventory and any normal pruning; do not clear global caches or
delete other workloads' images to manufacture a result. Preserve the existing
fixture bytes, dependency locks, base-image pins and SDK wheel.

## Helpers and execution order

1. [deployment-controller.py](deployment-controller.py) is the previously reviewed
   affinity controller with only its fixed release root changed. Stage the new
   wheel, this controller and the unchanged `scripts/repack_node_bundle.py` in the
   release directory. `stage` takes exact wheel, repacker and currently configured
   builder/sandbox bundle SHA-256 values. It preserves dependencies/native files.
   `check` and `apply` take the exact resulting staging-receipt SHA-256; apply
   requires idle work, backs up the venv/config and verifies automatic rollback
   health on failure. It updates only `node_package_root`. The 64 GiB relay
   budget, service policy and native components remain guarded.
2. [qualification-control.py](qualification-control.py) has `prepare`, `hold`,
   `state` and `release` actions for this reservation only. `prepare` requires an
   empty fleet and requests four builders with a 3,600-second demand expiry.
   This is a one-shot creation signal, consumed when the provider accepts the
   requested creations; its TTL is not a fleet lease. `hold` refreshes the owned
   demand after builders are ready. That demand remained present here because
   no further creation was needed. Use a new `--output` path for repeated holds.
   Receipt names are reserved before API mutations. Heartbeat inspection uses
   SQLite read-only mode, and SDK credentials stay on the gateway.
3. Use [initial-builder-check.py](initial-builder-check.py) on each fresh builder
   with its candidate bundle digest and expected module hashes. Save its output
   as `LOAD/builder-NODE_ID-initial.json`. The final audit requires four such
   receipts, active node agents, initially empty BuildKit caches and candidate
   hashes for `images.py`, `build_cache.py`, `environment_prepare.py` and
   `environment_builder.py`. BuildKit version/configuration are fixed to the
   preceding qualification.
4. Stage the unchanged `scripts/qualify_build_optimization.py` in the release
   directory. [run-qualification.py](run-qualification.py) validates the candidate
   deployment receipt, wheel, frozen wrapper/harness/SDK and all context bytes.
   Its default prints a plan. `--run` launches the same 48-request harness through
   the exact SDK driver unit above and records `LOAD/run-launch.json`. This unit
   name is essential: SSH-only driver launches cannot be separately attributed
   by the current process sampler. The service is bounded to 1,600 seconds; the
   existing wrapper limits the measured workload to 1,200 seconds and its child
   wait to 1,500 seconds. A failed/timed-out client still requires checking
   outstanding server builds before cleanup.
5. Sample gateway/builders through the measured phase and a bounded writeback
   tail, using public `/healthz`. Run [selection-report.py](selection-report.py)
   afterward with `--phase single-import-repeat --output LOAD/selection.json`.
   It reads only the owned 48 summaries/logs plus bounded registry GET/HEAD
   metadata. Require complete selector coverage and verify that exact matches
   report one import; distinguish that observation from actual `RUN` reuse.
6. Run one measured image from each recipe in an application smoke, verify its
   expected output and delete all three sandboxes. Save the exact smoke receipt.
   Run normal cache maintenance separately from the measurement, release the
   owned reservation and wait for all owned nodes to retire. Keep a ledger for
   automatically provisioned workers and any extra canary VM.
7. [final-audit.py](final-audit.py) checks 48 exact successful durable-history
   identities, three distinct recipe-bound/deleted smokes, frozen context
   verification, the successful named driver launch, four initial builder
   receipts, the installed candidate wheel and both future bundles. It also
   checks health, idle relay/lifecycle work, no active work/reservations/heartbeat
   rows, stopped driver/sampler units and provider absence of every owned node.
   Pass auxiliary IDs with `--known-node` and extra samplers with `--sampler-unit`.
   Its provider/API/SQLite operations are read-only; it creates one new receipt.

The frozen wrapper retains the historical `B2-selective-and-scheduling` label;
the candidate wheel/source receipt identifies this release's actual variable.
No helper silently reuses the measured phase or overwrites an earlier final
receipt. All existing affinity receipts remain independent.

After staging the candidate and preparing its four verified builders:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/build-cache-single-import-20260929-r1/run-qualification.py \
  --wheel-sha256 "$CANDIDATE_SHA256" \
  --fixture-manifests /work/ucloud-sandboxes/build-cache-single-import-20260929-r1/fixture-manifests.json
```

The command above only validates and prints the plan. Use the same command with
`--run` once the deployment owner starts qualification. Stage an unchanged copy
of `docs/benchmarks/build-load-2026-09-29/fixture-manifests.json` at the explicit
path above; its digest is checked. Earlier instructions named
`/work/ucloud-sandboxes/FROZEN-fixture-manifests.json`, but these helpers do not
assume that historical gateway path exists. Credentials are not passed on the
command line.

The deployment owner also verified this existing gateway inventory path:
`/work/ucloud-sandboxes/builder-execution-load-20260929/frozen-fixture-manifests.json`.
It can be passed explicitly instead of staging another identical copy.

After smokes and cleanup:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/build-cache-single-import-20260929-r1/final-audit.py \
  --wheel-sha256 "$CANDIDATE_SHA256" \
  --known-node "$EXTRA_OWNED_NODE_ID" \
  --output /work/ucloud-sandboxes/build-cache-single-import-load-20260929/final-state.json
```

Omit `--known-node` if there are no additional IDs; do not supply empty strings.
IDs visible in phase before/after snapshots and build owners are included
automatically. Initial empty-cache receipts establish cache state, not a runtime
capacity result.

## Frozen inputs and local validation

| Input | SHA-256 |
| --- | --- |
| Harness | `d0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec` |
| SDK 0.4.33 wheel | `d15b65fbb5e1570fde69cb9d571789a9b61d4418682efc17732bdc9c2ca8414c` |
| Qualification wrapper | `e9c60880598396d18aa377cc02b5659d9f4d3610cf2ae65411b829361beb288b` |
| Fixture inventory | `45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7` |
| BuildKit configuration | `ca46c1e0f19982ff9ef00df1b096674023b2d2a6603074abbcad7a8700c9de8d` |

Network-free audit, launch-plan and selection self-tests are available:

```sh
python3 docs/benchmarks/build-cache-single-import-2026-09-29/final-audit.py --self-test
python3 docs/benchmarks/build-cache-single-import-2026-09-29/run-qualification.py --self-test
python3 docs/benchmarks/build-cache-single-import-2026-09-29/selection-report.py --self-test
```

Report historical versus candidate batch/client/phase distributions, p95
regressions as well as improvements, selector coverage, observed application
execution, registry I/O and host pressure. Keep shared/replayed progress separate
from independent CPU work. Include the gateway-local SDK driver in whole-host
CPU and report its actual classified group coverage. This remains an image-build
qualification, not a 500/1,000-running-agent capacity test.
