# Production image-build load qualification — 2026-09-29

All **138 builds succeeded**, including a 48-request overload and 24 builds on
replacement VMs with empty local caches. Three representative published images
also passed execution checks inside sandboxes. The larger test exposed expensive
partial EROFS cache misses and queueing on one builder while others had capacity.

This run exercises the deployed shared BuildKit cache and EROFS publication path
with dependency-heavy applications, source edits, dependency changes, an overload
burst, and builder replacement. It measures the complete SDK build request,
including context packaging/upload, admission retries, builder queueing, image
build/push and immutable-environment publication.

## Workload and deployment

The production gateway has 4 dedicated CPUs and 16 GiB RAM. The build pool has
four CCX33 VMs, each with 8 dedicated CPUs and 32 GiB RAM. Production limits were
unchanged: four executing builds and eight admitted builds per builder. Sandbox
workers and image builders are separate roles.

| Fixture | Real build work | Application source |
| --- | --- | ---: |
| Python agent/data tools | 124 locked packages including NumPy, SciPy, pandas, Arrow, scikit-learn and Jupyter; a compiled C extension; bytecode compilation; data/model smoke checks | 1,500 modules |
| TypeScript development tools | 500 locked npm packages; ESLint, TypeScript, esbuild, webpack and Jest | 2,000 modules |
| TypeScript multistage runtime | The same compiler/test graph, then a slim runtime containing the bundled application | 2,000 modules |

These are generated applications using real public dependencies, not private
customer repositories. There are no padding files or artificial hashing loops.
The Node fixtures intentionally share compilation graphs; simultaneous requests
can share BuildKit work. They do not represent two independent compiles.
The largest observed images after the warm phase were approximately 662/419/76
MiB of compressed OCI layers and 1,118/705/130 MiB of EROFS payloads respectively.
These are measured compressed artifacts, not unpacked rootfs sizes.

The platform was `linux/amd64`. All base images are pinned to multiarchitecture
index digests in [base-pins.json](base-pins.json), and the
exact dependency locks are in [locks](locks). Application variants change one
late source file. Dependency variants change Python `rich` or Node `zod` and
use separately frozen dependency locks. [Fixture manifests](fixture-manifests.json)
record input hashes and expected executable results.
Python locks freeze package versions without wheel hashes; npm locks include
integrity hashes. The evidence does not promise identical future Python wheel
artifacts from a version pin alone.

BuildKit uses the pinned 0.33.0 docker-container driver. Each VM has its own
local store, with a 20 GiB GC target and 10 GiB minimum free-space policy. The
registry cache is shared across builders: `mode=min`, at most eight selected
imports per request, and retention targets of 64 owned tags, 32 GiB of unique
compressed referenced blobs, and seven days. These are retention/GC policies,
not strict physical disk quotas.

## Results

All six phases completed successfully, with a distinct image ID and build UUID
for every request. All 138 terminal summaries were persisted in the gateway's
operator-history SQLite database. This is separate from SDK build-status polling,
which queries live builders and can return 404 after their retirement; no public
historical status fallback is claimed.

| Phase | Requests / concurrency | Client p50 | Client p95 | Batch duration | Recovered submit HTTP 503s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Empty local stores, new fixture recipes | 12 / 12 | 112.3 s | 162.8 s | 168.6 s | 0 |
| Repeat the same 12 contexts twice | 24 / 24 | 14.5 s | 61.7 s | 63.2 s | 0 |
| New application edits | 24 / 24 | 47.6 s | 97.9 s | 99.6 s | 3 |
| Dependency changes | 6 / 6 | 62.4 s | 128.7 s | 128.7 s | 0 |
| Overload with new application edits | 48 / 48 | 77.3 s | 162.5 s | 172.2 s | 133 |
| Replacement VMs after cache pruning | 24 / 24 | 96.8 s | 146.5 s | 149.1 s | 0 |

Every request uses a new image name and has a distinct build UUID; these are not
completed-image API shortcuts. The first pool's BuildKit stores were verified at
0 B before the cold phase. The registry already contained three unrelated cache
entries, so “cold” does not mean an empty production registry.

The overload reached exactly 32 admitted and 16 executing builds, with maxima
of eight/four on each owner. All 48 requests succeeded at 16.7 builds/minute over
this finite burst. Worker queue p95 was 108.4 seconds and submission p95 was
55.7 seconds. The SDK handled all 133 submit 503 responses; the harness records
status codes, not their error bodies, so it cannot classify every 503's cause.
Percentiles of separate phases must not be added together.

Full per-recipe distributions, observed concurrency and phase timers are in
[the generated report](build-load-report.md); its [JSON](build-load-report.json)
preserves counts and maxima. Cold/warm phases differ in concurrency and share
work, so their latency ratio is not a controlled estimate of optimization gain.

## What the load exposed

1. **Partial EROFS hits still materialize too much.** Two warm Python builds
   reused five groups but pulled/extracted the Docker image for about 33.7 seconds
   to produce one missing 5.3 MB group. Selective materialization of missing
   groups is a concrete next optimization. It must preserve lower-layer,
   whiteout, ownership and metadata semantics.
2. **Long per-builder queues leave capacity unused elsewhere.** In the overload,
   four TypeScript requests on one owner waited about 109 seconds. At 100 seconds,
   they remained queued while fleet execution was only 13/16; the final tail was
   concentrated on that owner. Shorter local admission queues or central work
   assignment deserve a controlled follow-up. [Queue analysis](queue-analysis.md)
   includes exact intervals and a reproducible 23.88-second period of queued
   work behind a full owner while peers had free execution slots.
3. **A cache marker is not proof of an instant build.** Cached results still
   need downloading/extraction. Multistage requests also waited for a concurrent
   compile and later printed `CACHED`. One warm Python request imported its exact
   older cache yet rebuilt late steps on another cached dependency chain. Merely
   increasing the eight-import limit is not a demonstrated fix.
4. **Equivalent outputs can get different OCI identities.** Inspected matching
   cold/warm source and compile layers had identical file contents but different
   timestamps. EROFS output bytes were identical, but the source-chain lookup
   still missed. Coherent cache selection and deterministic build outputs are
   useful follow-up experiments; silently discarding arbitrary user timestamps
   would change image semantics.

See [cache-miss analysis](cache-miss-analysis.md) for measured layer comparisons
and explicit distinctions between evidence and BuildKit solver inferences.

Gateway API CPU averaged only 0.012–0.053 cores across the first five phases.
During overload, whole-gateway CPU averaged 0.341 cores, with p95 1.605 and
maximum 2.110 of four cores, including the local SDK load driver. Builders
reached roughly 6.4–6.6 occupied cores at p95 on eight-core machines. Registry
storage showed burst pressure: cold writes reached 250.7 MiB/s at p95 with
180.5 ms p95 request latency and 56.7% p95 gateway I/O pressure. These data
support reducing materialization, publication I/O and queueing before spending
more gateway CPU or rewriting its HTTP service for this build workload.

[Host findings](host-findings.md) retain coverage, process attribution, memory,
pressure, I/O and health evidence. Invalid disk busy-time counters are explicitly
excluded; they cannot establish utilization. All 129 observed gateway health
probes across the six phase windows succeeded. No OOM kills or swap
activity were observed in those covered intervals. Replacement gateway CPU was
0.254 cores on average and 1.130 at p95; its registry volume still wrote 4.08 GiB
with 35.65 ms request-weighted I/O latency.

## Cache storage and pruning

Before pruning, 114 image records referenced 46.10 GB of OCI layers when counted
per image, but only 2.05 GB of unique OCI layer blobs. Their EROFS payloads were
77.83 GB counted per image versus 3.52 GB unique. Shared cache references totaled
2.08 GB; 2.05 GB of those blobs also belonged to the measured images. Descriptor
accounting excludes filesystem metadata, uploads and pending garbage collection.

The [first prune](cache-prune-first.json), executed through the normal registry
maintenance lock and CLI with repository prefix `ucloud-build-cache`, reduced
117 tags to the configured 64 entries. It deleted 50 cache manifests because
some manifests had multiple tags, retained 1.63 GB of referenced cache blobs,
and removed zero image records. No physical registry garbage collection was
forced. Removing cache references does not promise an immediate disk-space drop,
especially while final images still reference the same blobs.

After that prune, all four original builders retired through the normal
autoscaler; [the retirement receipt](first-pool-retired.json) has an empty fleet
and no sandbox/build demand. Four new builder IDs were provisioned and each
BuildKit store was again measured at 0 B. The replacement phase mixes 12 older
contexts (application revisions 1–4) and 12 recent contexts (25–28) on this pool.
Its shared registry/EROFS cache remains populated, as intended for scale-down
qualification.

All 24 replacement builds succeeded. Older contexts had client p50/p95
96.8/149.1 seconds; recent contexts had 96.9/128.3 seconds. The phase reused
82 EROFS groups and rebuilt 22, with two full Docker-pull skips. It proves
successful recovery after builder retirement and bounded cache retention; it
does not show warm-store latency after replacement. Both VM state and retained
tags changed, so this experiment cannot isolate pruning's performance effect.
The [independent review](review.md) details cohort accounting and claim limits.

The [final prune](cache-prune-final.json) reduced 88 tags to 64 by removing 21
cache manifests, with no image-record removals. The [final inventory](image-inventory-final.json)
confirms 64 entries referencing 1.27 GB of unique compressed cache blobs; all but
0.274 MB are also referenced by the measured images. Across all 138 images,
OCI layers total 55.81 GB counted per image versus 2.08 GB unique, and EROFS
payloads total 94.22 GB versus 3.64 GB unique. Actual registry filesystem usage
grew by 5.79 GB to 155.15 GB (15.48% of the volume). Logical descriptor totals
and physical filesystem usage are different measures.

## Functional checks and cleanup

[Three representative replacement images](smoke-replacement.json), one per
recipe at application revision 1, passed their real Python data/native/model or
Node application checks inside production sandboxes. All three temporary
sandboxes were deleted. This is representative execution validation, not smoke
coverage of every image, revision or dependency variant.

Both preparation holds were released and all nine host samplers were stopped.
Their complete records are retained as gzip JSONL in [telemetry](telemetry).
The 138 managed test-image aliases, frozen contexts and gateway-local build
receipts/logs remain as qualification artifacts under the normal lifecycle.
Cache pruning does not remove those image aliases or force physical GC.

The [final audit](final-state.json) at 08:38:11 UTC confirms all eleven observed
test builder/warmed-worker VMs are absent from the provider, an empty fleet,
zero sandboxes and active builds, no prepared or pending demand, and an inactive
gateway sampler. Gateway/relay HTTPS health and metrics pass. Read-only exact-ID
queries confirm all 138 successful summaries remain in the operator-history
database after VM retirement. [The audit script](final-audit.py) stays on the
gateway for credentialed checks and only returns allowlisted metadata.

## Reproduction and interpretation

The reusable workload description is [build-load-fixtures.md](../build-load-fixtures.md).
The fixture generator, SDK runner, image validator and telemetry tools are in
`scripts/build_load_*.py` and `scripts/live_build_load_benchmark.py`. Run the SDK
driver on the gateway with its deployed Python and SDK 0.4.33 wheel. Credentials
are read there and are never copied into this report or to the local workspace.
For another run, use a fresh root, preparation ID and image-name prefix; this
dated runner intentionally refuses to overwrite a phase directory.

Prepare at most the configured four builders. Preparation is consumed when VMs
are created, so renew the hold after they are ready. Record empty-store evidence
before builds, then sample the gateway and each builder every two seconds.
Start the client as a separate `ucloud-build-load-client-*` systemd unit so its
CPU can be attributed. Release only the run's preparation ID after measurement;
stop and preserve its telemetry before idle retirement. Execute the fixture
manifest's smoke command in a sandbox from each published recipe and delete
those temporary sandboxes.

The [reproduction check](fixture-reproduction.json) regenerated six contexts
(each recipe at application revision 1 and its dependency-change variant) from
the preserved locks and base pins. All six input hashes match the measured
fixtures. [Validation metadata](validation.json) records tool/input hashes and
the final consistency checks.

Recompute reports locally, including from gzip-compressed telemetry:

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/build-load-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-load-2026-09-29
```

This test exercises real package installation, compilation, registry traffic,
cache reuse and EROFS construction. It does not qualify 500–1,000 running agent
sandboxes, LLM relay traffic, or a sustained mixed workload. The SDK and health
probes originate on the gateway, so WAN latency is absent and driver CPU is
included in whole-host measurements. A sandbox worker was automatically warmed
by build activity even while sandbox demand was zero; its provisioning is a
whole-host cold-phase confounder documented in [the receipt](build-triggered-worker.json).
