# Build-cache input affinity — 2026-09-29

This release selects shared BuildKit caches using verified build inputs. It was
deployed at **13:10:36 UTC**, with healthy gateway, relay and metrics checks.
**Both 48-build phases passed:** seed took 125.635 seconds and the fresh-builder
repeat took 98.594 seconds. All 48 repeat builds selected an exact-affinity cache.
The controlled cache/invalidation proof, all three application smokes, and both
cache-import diagnostics passed. The repeat pool was released at 13:55:36 UTC;
the [final audit](final-state.json) passed at **13:58:21 UTC** with all ten owned
nodes retired, 96 successful histories, three deleted smoke sandboxes, healthy
services and no remaining work, reservations or active samplers. This release's
measured policy imported at most eight caches. The later diagnostics support a
separately qualified exact-cache-only candidate.

The previous [execution-model release](../builder-execution-2026-09-29/README.md)
completed 48 builds in 115.495 seconds. Build-and-push median/p95 remained
27.074/44.012 seconds. This release targets avoidable repeated execution
when a useful registry cache exists but falls outside the eight imported entries.
It does not make a required lint, compile or test command intrinsically faster.

[Retained BuildKit progress](baseline-buildkit-progress.md) identifies the main
remaining work: tools and multistage application instructions executed 13 and
15 times, averaging 30.077 and 28.767 seconds per execution. Image/cache export
averaged roughly 0.3–1.1 seconds. Python's large dependency/native `RUN`-labelled
durations were predominantly cached-layer materialization. These vertex timings
can overlap and must not be added as wall-time shares.

[Candidate source verification](candidate-source.json) records 163 packaged
files matching wheel SHA-256
`21c77e27c4a064c6a52e93d8af870611d7f56f0559adb609172d9558b5366054`.
[Staging](staging-receipt.json) preserved dependency/native inventories in both
future-node bundles. The [deployment receipt](deployment-receipt.json) records
only `node_package_root` changing, healthy HTTPS/metrics and the unchanged 64 GiB
relay budget. The release's [controller](deployment-controller.py) retains paired
gateway/configuration rollback under the release root. [Local verification](test-verification.md)
is separate from the live load, correctness and cleanup gates below.

The deployed selector puts the newest matching recipe and exact-input `bc2`
entry first, then the newest same-recipe entry, distinct recipe heads and remaining
recent entries, without duplicates. Input affinity uses the verified uploaded
archive identity, normalized Dockerfile path and build arguments; destination
image names are excluded. Existing `bc1` entries remain importable and subject to
the same bounded retention policy. This is a lookup hint: BuildKit still validates
the build graph and every output follows the existing image-publication path.
Fallback recipe diversity is also a policy change, so full-batch results must be
attributed to the combined selector unless a separate comparison isolates it.

## Measured results and remaining misses

[The seed](affinity-seed/summary.json) ran from 13:16:05 to 13:18:11 UTC and
[the repeat](affinity-repeat/summary.json) from 13:27:54 to 13:29:33 UTC. Each
completed 48/48 builds and retained 48 durable-history records. Their qualification
receipts verify the same frozen contexts. The repeat used four new builders with
empty local BuildKit stores: `167968139`, `167968141`, `167968156` and `167968157`.
The seed pool was `167966880`, `167966883`, `167966886` and `167966887`.

| Measurement | Previous execution release | Affinity seed | Affinity repeat |
| --- | ---: | ---: | ---: |
| Successful builds | 48/48 | 48/48 | 48/48 |
| Batch completion | 115.495 s | 125.635 s | 98.594 s |
| Client p95 | 112.104 s | 116.639 s | 97.275 s |
| Submission/admission p95 | 79.793 s | 88.266 s | 65.685 s |
| Build-and-push median | 27.074 s | 32.266 s | 21.647 s |
| Build-and-push p95 | 44.012 s | 47.592 s | 50.656 s |
| Environment-publication p95 | 3.738 s | 3.992 s | 4.249 s |
| Submit HTTP503 responses | 560 | 668 | 309 |
| Exact-affinity selector observations | Not available | 0/48 | 48/48 |

The repeat batch was 14.6% faster than the previous historical run and 21.5%
faster than seed. **This is not a universal latency improvement:** build-and-push
p95 increased to 50.656 seconds, and environment-publication p95 also increased.
Seed/repeat changes cache population; the historical comparison additionally
changes runtime, placement and cache chronology. [The performance report](performance-comparison.md)
preserves phase coverage and these distinctions rather than treating aggregate
overlapping build durations as CPU time or batch-wall components.

[Seed selection](affinity-seed-selection.json) and
[repeat selection](affinity-repeat-selection.json) both cover all 48 logs and
record eight imports per build. Each export digest resolved uniquely to a `bc2`
tag in its captured inventory. An exact-affinity selection does not guarantee
that every application instruction is reused: several repeat builds still
executed expensive work.

[The retained cache-record comparison](cache-record-comparison.json) examines
tools case `001`: the exact seed cache appears in its repeat import, yet progress
shows a 3.7-second source `COPY` and 36.9-second application `RUN`. The two
SHA-verified cache configurations contain the same 13 record keys and input
links/selectors, including final-stage records. Eleven complete records match;
two have new output blobs/timestamps. This case does not support missing final
records under `mode=min` or a changed graph identity as the explanation.
Those initial observations did not establish the cause. The subsequent
diagnostics narrow the useful fix to import selection rather than requiring
daemon isolation; they do not establish a particular upstream issue as the
fleet's root cause.

[The first concurrency diagnostic](CACHE-CONCURRENCY.md) rejects a broader import
set as the next deployment based on this test: four concurrent requests over 12
tools cases took 73.380 seconds with eight imports (six execution/six cached
observations), versus 120.306 seconds with 64 imports (12 execution/zero cached
observations). A separate one-case, one-import serial arm was cached in 1.569
seconds. That serial comparison changes both concurrency and import count, so it
does not identify concurrency as the sole cause.

[The second diagnostic](cache-diagnostics.md#second-diagnostic-results) replayed
the exact same immutable inventory and contexts on fresh stores. Two serial
eight-import requests both reran the application instruction, taking 32.282 and
31.829 seconds. With **one exact import per request**, all twelve requests were
cached at concurrency four on a shared daemon (3.620 seconds); four exclusive
daemons also cached all twelve (3.226 seconds). All ten private drivers across
both diagnostics were removed without errors, and frozen manifest checks passed.
The multi-import miss can therefore occur without concurrent solves, while
exact-only imports preserved reuse under concurrency on one daemon in this test.

The graph comparison shows overlapping tools/multistage keys with different
result-layer coverage. Combined-cache key/result selection is a plausible
supported explanation for the missed reuse; no upstream bug is claimed fixed.
These results support testing exact-only imports when an affinity match exists,
retaining fallback selection otherwise. **The 3.620-second cache-only diagnostic
is not a 48-build production result**: it omits image/cache export, environment
publication and sandbox validation. Its proposed runtime change requires a new
full pipeline qualification and is outside this release's 98.594-second result.

Concurrent solves can share/replay progress. Repeat cases `005` and `020` include
both execution and cached application vertices, and separate case logs can echo
the same durations. Do not interpret per-log vertex totals as independent CPU
work, count an entire build as a hit merely because it contains a `CACHED` marker,
or add nested/materialization timings to enclosing build duration. Controlled
concurrency diagnostics are outside these frozen 48-build measurements.

[The standalone proof](CONTROLLED-PROOF.md) passed from 13:20:40 to 13:21:24 UTC:
the old result outside baseline's eight imports executed under recency selection
and was cached under affinity on independent empty stores, with identical proof
file bytes. Changed source and a consumed build argument both reran and produced
the new expected output. Its three owned drivers and ten cache manifests were
removed with zero cleanup errors. These tiny-fixture timings do not predict
representative application speedups.

[All three repeat application smokes](smoke-affinity-repeat.json) verified the
expected recipe output, exited successfully and were deleted by 13:33:21 UTC.
The [seed reservation](pool-seed-release.json) was released at 13:22:43 UTC.
The [repeat reservation](pool-repeat-release.json) was released at 13:55:36 UTC
after the diagnostics. The [resource ledger](resource-ledger.json) is
complete against the [13:58:21 final audit](final-state.json): its ten unique node
IDs exactly match the audit's provider checks, and none remained. At audit time,
the gateway had zero sandbox/build/heartbeat state and no pending reservations;
relay/lifecycle work was idle, the sampler was inactive, and gateway/relay/metrics
health passed.
All 96 exact build-history UUIDs are successful and all three required smokes
passed and were deleted. These cleanup checks do not make a running-agent
capacity claim.

### Host measurements and SDK attribution gap

[The host report](host-findings.md) retains 99.2% gateway coverage for seed and
97.0% for repeat. Whole-host CPU averaged 0.405/0.496 occupied cores, with p95
1.575/1.770; gateway API CPU averaged 0.070/0.068 and registry CPU 0.110/0.139.
All 31 sampled public `/healthz` probes passed. Registry volume writes within
the covered client windows were 0.620/0.441 GiB, with I/O-weighted await
3.72/3.76 ms. Buffered writeback outside those windows is separate.

The SDK harness for these two phases was launched through SSH, outside the
`ucloud-build-load-client*.service` cgroup recognized by the sampler. **Separate
SDK CPU attribution is unavailable, not zero.** Whole-host CPU includes its work;
API and registry measurements remain valid. The raw sampler's unclassified
driver zeros are preserved as raw evidence, but both derived reports show `N/A`
and JSON marks the group unavailable with launch provenance. Do not subtract
those zeros or infer that archive creation consumed no CPU.

Across the eight builders, phase mean CPU ranged from 2.604 to 4.512 of eight
cores and p95 from 5.760 to 6.815. Minimum sampled available memory was 25.45 GiB
on builders and 12.90 GiB on the gateway; no OOM or swap activity was observed
in covered intervals. Invalid disk-busy counter jumps are explicitly excluded.
These phase summaries do not justify broader running-sandbox capacity claims.

The local [attribution postprocessor](host-attribution.py) applies the declared
measurement limitation only to `affinity-seed` and `affinity-repeat`. It changes
derived driver fields, preserves other host metrics and leaves all raw telemetry
untouched. Reproduce in this order after refreshing source artifacts:

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/build-cache-affinity-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-cache-affinity-2026-09-29
python3 docs/benchmarks/build-cache-affinity-2026-09-29/host-attribution.py
```

## Frozen scope

| Item | Qualification value |
| --- | --- |
| Release root | `/work/ucloud-sandboxes/build-cache-affinity-20260929-r1` |
| Load root | `/work/ucloud-sandboxes/build-cache-affinity-load-20260929` |
| Measured phases | `affinity-seed`, then `affinity-repeat` |
| Image prefixes | `bl20260929-affinity-seed-`, `bl20260929-affinity-repeat-` |
| Workload | Three recipes × `app-change-5` through `app-change-20`, 48 builds per phase |
| Admission | 48 client submissions, four builders, four admitted builds per builder |
| Builder execution | Existing pinned BuildKit, four-way worker parallelism |
| Registry cache | At most eight imports, `mode=min`, unchanged compression and retention |
| SDK | Frozen 0.4.33 wheel; no SDK/archive changes in this comparison |
| Repeat smokes | `smoke-affinity-repeat.json`: one successful, deleted sandbox per recipe |
| Default sampler | `ucloud-affinity-load-monitor`, public `https://77.42.92.27/healthz` |

The frozen harness SHA-256 is
`d0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec`;
the SDK wheel SHA-256 is
`d15b65fbb5e1570fde69cb9d571789a9b61d4418682efc17732bdc9c2ca8414c`.
The fixture inventory SHA-256 is
`45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7`.
The existing qualification wrapper verifies every context and lock byte before
running the server-local harness. Its legacy `B2-selective-and-scheduling` label
does not describe the isolated variable in this cycle; candidate source and wheel
receipts must identify the affinity implementation explicitly.

## Qualification protocol

1. Stage and validate the exact candidate wheel and both future-node bundles.
   Preserve dependencies, native artifacts, service settings and the 64 GiB relay
   budget. Record the idle deployment and health gates independently of load.
2. Provision four fresh seed builders. Capture node identity, installed source
   hashes, BuildKit version/configuration and empty local BuildKit caches before
   submitting the frozen 48 contexts as `affinity-seed`. Retain all terminal build
   UUIDs and phase timings. Existing registry caches are allowed and inventoried;
   this phase is not described as globally cold.
3. Capture a complete immutable cache inventory after seed and before repeat.
   Keep tag timestamps, manifest digests, recipe/affinity identifiers and the
   exact baseline/candidate selections for an early seed case. Release the seed
   reservation and retire its four builders normally.
4. Provision four different builders with empty local BuildKit caches. Run the
   same frozen 48 contexts under new image/build identities as `affinity-repeat`.
   Local layer reuse within this burst is expected; it must not be confused with
   having prewarmed the new builder pool.
5. Run the bounded old-cache and invalidation proofs below separately from the
   batch timing. Execute one published repeat image per recipe in a sandbox,
   verify its expected result and delete each sandbox.
6. Run normal cache pruning with the unchanged policy, release owned reservations,
   stop samplers and wait for owned nodes to retire. Preserve the exact resource
   ledger, including any canary or smoke worker absent from phase snapshots.
   Run the read-only [final audit](final-audit.py) against both phase receipts.

Seed versus repeat measures the effect of newly populated affinity entries and
changed cache state, not an isolated runtime A/B. The historical execution-model
run is contextual evidence only. Report each phase separately, and report any
required re-run under a new identity rather than overwriting a failed attempt.

## Controlled cache and correctness proofs

An early seed must be outside the **baseline selector's** imported eight entries,
not merely outside the eight globally newest tags. The old algorithm first
selected the newest same-recipe cache and then filled remaining slots by global
recency. Freeze its eight selected references and the candidate references from
the same inventory, with manifest digests. Use a target whose exact-input cache
the candidate includes and this baseline excludes; do not rewrite timestamps or
increase the production retention budget to manufacture the condition.

Use empty local BuildKit stores for the measured witness arms, preserving identical
inputs, base-image digests, import count, export mode and build command. Record
whether the application `RUN` executes or is `CACHED`, and verify the resulting
application output. A selection-only counter proves lookup behavior; an observed
cache hit on a fresh builder proves reuse. To claim that affinity caused a saved
execution, retain both baseline and candidate witness outcomes: another imported
cache could contain the same execution result. Pin the witness inventory before
repeat exports alter recency, and label any canary that warms a measured builder.

Run two small semantic canaries using disposable owned contexts: change source
consumed by a `COPY` followed by `RUN`, and separately change an `ARG` actually
consumed by a `RUN`. Both must execute the affected step and yield the changed
result. An unused argument is not an invalidation test. Also retain successful
legacy `bc1` import evidence. Neither affinity collisions nor stale hints may
authorize reuse of an image or bypass BuildKit graph validation.

Normal pruning retains at most 64 cache tags. An inventory can therefore change
between proof selection and execution. If the witness was pruned, record the
miss and choose a still-valid controlled case; do not call a stale reference a
correctness failure. Deleting manifests is not proof of physical disk reclamation,
and this qualification does not invoke global registry garbage collection.

The read-only [selection report](selection-report.py) reads only the exact 48
owned phase summaries/logs and makes bounded registry GET/HEAD requests. It
records selector observations, recognized cache import tags and logged immutable
export digests, then snapshots validated owned tag/digest metadata. A digest can
have multiple `bc2` aliases: only a unique join is reported as an export tag;
otherwise ambiguity is retained. Neither an affinity-match counter nor a joined
tag is a substitute for the separate observed `RUN` cache-hit proof. Raw logs,
commands and credentials are excluded from this receipt.

Run on the gateway after seed, before repeat modifies cache recency; use a new
output name and `--phase affinity-repeat` after repeat:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/build-cache-affinity-20260929-r1/selection-report.py \
  --phase affinity-seed \
  --output /work/ucloud-sandboxes/build-cache-affinity-load-20260929/affinity-seed-selection.json
```

Its self-test covers strict input ownership, sanitized projection, conflicting
selection observations, legacy tags, export alias ambiguity and missing-log
coverage. A local replay of all 48 retained historical logs recognized one export
digest and legacy imports per case, with no affinity-selection field, as expected
for that older runtime. No registry request was made during these local checks.

## Measurements and interpretation

Retain client wall/submission time, retry counts, accepted-request latency,
durable `preparation_ms`, queue, BuildKit build-and-push, environment publication,
cache preparation/mount and selective-child timings with numeric coverage.
Measure BuildKit progress separately for import, required application commands,
image export and cache export. Keep complete-cache-hit publications separate from
selective misses; missing nested timings are not zero. Capture CPU by service,
registry volume bytes/await/PSI, builder CPU/memory/PSI and `/healthz` results.
Exclude known invalid disk-busy counters from capacity conclusions.

SDK archive work is a material fixed comparison variable. In the previous run,
all 48 context probes hit existing gateway uploads and no context upload occurred.
The 16 submissions without admission retries still took median 7.705 seconds;
subtracting measured request-header durations left median 7.359 seconds. The
frozen SDK constructs and hashes its deterministic gzip archive before probing
for an existing context. That residual also includes scheduling and response-body
processing, so it is not an isolated compression measurement. Changing the SDK
or fixture layout during this cycle would confound the cache comparison.

Admission retries are also included in client submission time. The previous run
had 560 HTTP503 submit responses and two-to-two-and-a-half-second client backoff;
accepted submit header latency had p95 0.879 seconds and node preparation p95
0.831 seconds. A shorter repeat batch can reduce admission waiting without
changing that protocol. Do not attribute all client improvement to BuildKit time.

Prefer one continuous gateway sampler spanning both phases and separate builder
files per node. If gateway captures are split, analyze explicit per-phase files
instead of treating a missing disjoint interval as covered. Keep a bounded tail
after each phase for buffered registry writes, separate from the client window.
These are image-build tests, not 500/1,000-running-agent capacity qualifications.

## Audit interface and local checks

The audit requires 96 distinct successful build UUIDs, exact corresponding durable
history identities, the same 48 recipe/context hashes in both phases, disjoint
four-builder pools and three distinct recipe-bound repeat smokes. Seed has no
smoke gate. It verifies the installed package and future bundles against the
specified wheel, frozen harness/SDK bytes, health, idle relay/lifecycle state,
no active builds/sandboxes/reservations, no heartbeat rows, stopped sampler units
and provider absence of every known owned node. Database reads use SQLite
`mode=ro`; no `ControlStateStore` constructor runs. It creates one new mode-0600
receipt and never overwrites earlier evidence.

The final audit's node union comes from both phase summaries, initial fleet
snapshots and repeat build owners. Pass every additional canary/smoke node from
the resource ledger explicitly; a final empty gateway fleet cannot prove an
unlisted provider instance has been removed. The audit is not the cache-hit or
invalidation proof, and does not turn final health into in-burst health evidence.

Local, network-free validation:

```sh
python3 docs/benchmarks/build-cache-affinity-2026-09-29/final-audit.py --self-test
```

After the deployment owner has collected both phases and completed cleanup:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/build-cache-affinity-20260929-r1/final-audit.py \
  --wheel-sha256 "$CANDIDATE_SHA256" \
  --known-node "$CANARY_NODE_ID" "$SMOKE_NODE_ID" \
  --output /work/ucloud-sandboxes/build-cache-affinity-load-20260929/final-state.json
```

Omit absent extra IDs rather than supplying empty strings. Add `--sampler-unit`
for any other sampler used; the default affinity sampler is always checked.
The local self-test covers seed without smokes, malformed/duplicated identities,
wrong or repeated smoke images/recipes, input mismatch, reused builder pools,
exact history lookup, read-only/missing database behavior, failed cleanup gates
and installed-wheel mismatch. Passing it is a harness check, not production
qualification.
