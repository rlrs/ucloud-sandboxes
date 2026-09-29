# Selective EROFS materialization and builder admission qualification

This is the prepared protocol and tooling, not a claim that the checks below
have run. Preparation did not mutate production. Preserve the measurements and
actual release digests from each execution beside this document.

The two changes address different costs: partial EROFS misses currently pull
and unpack cached lower OCI layers; accepting eight builds per four-slot builder
can strand admitted work behind a busy owner while another builder becomes free.
The intended new limit is four **nonterminal** builds, counting preparation,
and four executions per builder. More retryable submission 503s are expected;
complete client latency and correct completion determine whether that helps.

## Gate 1: exact EROFS bytes on the same immutable OCI source

Use `scripts/qualify_selective_environment.py` on one owned, otherwise idle
builder. Load the candidate wheel with the provisioned builder Python and
`PYTHONPATH`; do not replace that builder's running package for this test. Keep
its existing producer key and trust files on that host. Record the candidate
wheel SHA256 and builder identity separately. The receipt fingerprints the
actually imported `environment_builder` source, including when loaded from a
wheel.

Select an already-published Python application image from the prior benchmark
(for example a successful warm/app record). Read its source OCI manifest digest
from its build receipt or registry; freeze that digest before testing. The
source manifest must still reference every original OCI layer, and its existing
EROFS components must all be cached. A source image's environment annotation
does not replace the OCI layers.

```sh
PYTHONPATH=/work/ucloud-sandboxes/CANDIDATE.whl \
  /work/ucloud-sandboxes/node-venv/bin/python \
  /work/ucloud-sandboxes/qualify_selective_environment.py \
  --repository ucloud-managed/EXISTING-IMAGE-ID \
  --manifest sha256:IMMUTABLE-SOURCE-MANIFEST \
  --work-root /work/ucloud-sandboxes/selective-abba-UNIQUE-ID \
  --expect selective --order ABBA
```

Resolve the actual provisioned Python path before execution; the path above is
an example, not a discovery mechanism. The helper deliberately refuses an
existing work directory, a mutable source tag, an uncached image, or selection
of the base group. `--group -1` selects the last group by default.

The two arms differ only in the candidate helper:

* A patches `_materialize_registry_groups` to return `None`, taking the
  existing Docker pull/extraction path after the same forced cache miss.
* B runs the candidate normally after that same forced miss.
* Both arms force only the selected group's lookup to miss **in memory**.
  All other groups use the real signed cache, with tag refresh suppressed.
* A fail-closed registry adapter permits only manifest/blob reads. Publication
  is captured locally, authenticates the signed component, and independently
  hashes the actual EROFS file before that temporary file disappears. There
  are no shared tag writes, source annotation changes, or registry deletions.
* Every ordered signed component must equal the existing Docker-produced
  reference, including image digest/size, chunk digests, parent, source layers,
  format, and signature. Source config, image ID, and diff IDs must also match.
  Exactly one selected group must be rebuilt. Matching EROFS bytes covers
  filesystem semantics, metadata, whiteout encoding, and hardlink topology
  more strongly than a file-content-only comparison.
* A selective case requires `selective_materializations == 1` and no Docker
  fallback in both B trials. An unsupported case uses `--expect fallback`
  and requires B to enter the Docker path while preserving exact output.

The ABBA wall times include local EROFS hashing in both arms. Docker's local
image cache warms across trials; this is byte/path qualification, not proof
of an end-to-end latency improvement or a cold network comparison. The receipt
contains raw per-arm phase metrics so that limitation remains visible.

Required semantic matrix before publishing layout-1 components from this path:

| Input in the forced missing group | Required result |
| --- | --- |
| Ordinary application append and edit with explicit parent metadata | Selective path; exact EROFS equality |
| Nested directories, file uid/gid/mode, executable, safe relative symlink | Selective where supported; exact metadata/EROFS equality |
| Hardlinks to regular members within the same tar | Selective where supported; exact hardlink/EROFS equality |
| Deletion whiteout hiding an existing lower file | Docker fallback; deleted path absent in sandbox |
| Opaque directory hiding lower children | Docker fallback; hidden children absent, new children present |
| Hardlink whose target exists only in a lower tar | Docker fallback; contents/link semantics match baseline |
| Missing explicit parent metadata or unsupported xattr/PAX/special-file form | Docker fallback or the existing explicit image rejection; no newly signed divergent component |

Inspect the immutable layer tar headers to establish which case was actually
created. A Dockerfile `ln`, directory recreation, or `rm` does not by itself
prove the exported OCI tar contains a cross-layer hardlink or opaque marker:
the exporter may flatten that operation. Do not count such a case as coverage
until the selected tar contains the intended encoding. Use the old producer to
prime these owned canaries, then pin their source digests and force only the
affected group missing with this helper. Never delete shared component tags to
manufacture a miss.

`scripts/qualify_selective_semantics.py` prepares this concrete matrix on the
owned idle builder. Stage it beside `qualify_selective_environment.py` and run
with the same candidate Python/PYTHONPATH, `--repository`, `--manifest`, and a
new `--work-root`. It appends an explicit common lower tar and a case tar to
the frozen base, reuses existing blobs in the same owned repository, removes
the inherited environment annotation, and writes only unique UUID `qual-*`
source tags. Cases are `links`, `whiteout-opaque`, `missing-parent`, and
`cross-layer-hardlink`; `--cases` can select a subset. The two latter cases are
separate, so one rejection cannot conceal missing coverage of the other.

Each new tar is under 64 KiB compressed / 128 KiB unpacked; aggregate new source
layer/config uploads are bounded to 2 MiB. This bound excludes Docker-produced
EROFS components, which include the original source's final grouped layers;
their actual bytes are recorded in baseline metrics. The helper first publishes
only missing canary EROFS groups using Docker, suppressing existing cache tag
refresh. It then runs exact AB comparison, retaining actual tar header/digest
proof, metadata, path results, and receipts. It removes only each unique local
Docker image reference with `--no-prune` and no force flag. Source test tags and
signed baseline components remain as evidence; no registry cleanup or GC runs.
If Docker rejects a cross-layer-hardlink source, that case remains incomplete,
not a successful fallback qualification. Rootfs runtime smoke remains a
separate gate below.

The SDK runtime gate is prepared in `scripts/qualify_selective_runtime.py`.
Stage it beside `qualify_selective_semantics.py` on the gateway and copy only
the semantic receipt from the builder. The selected source tags currently have
no environment annotation, and managed-registry references bypass automatic
external import. This helper therefore submits a tiny normal SDK-managed
`FROM <same repository>@<pinned receipt digest>` build under a fresh owned
image ID before creating each sandbox.

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/qualify_selective_runtime.py \
  --receipt /work/ucloud-sandboxes/semantic-canaries.json \
  --output-root /work/ucloud-sandboxes/semantic-runtime-UNIQUE-ID \
  --cases links whiteout-opaque missing-parent
```

An incomplete source receipt is accepted only with an explicit selection, and
every selected case must already have exact EROFS equality. The cross-layer
hardlink case may be excluded if the existing Docker importer rejected it;
that is recorded as an existing rejection, not supported runtime coverage.
The helper verifies run/tag ownership, immutable source digest, uncompressed
tar identity, and literal tar header proof before issuing any API call.

It runs one 1-CPU/512-MiB sandbox at a time with 1-GiB scratch and a 600-second
TTL. Its security user is explicitly `23123:23124`, the fixture's authored owner;
the SDK default `1000:1000` cannot traverse those correctly preserved 0750
directories. Other security defaults remain unchanged. The guest asserts and
records its effective uid/gid. Guest checks verify content, size, uid/gid/mode, EROFS fixed mtime zero,
actual inode identity for hardlinks, inert symlink text, executable behavior,
and deleted/opaque path absence. The unspecified directory header in the
missing-parent case is observed without assigning it invented OCI metadata;
the prior byte comparison already checks Docker's inferred result. The
20-minute suite budget excludes bounded cleanup attempts. Credentials remain
in the server-local SDK client; errors record only class/status/code and the
owned fixture assertion output. Cleanup checks labels, deletes only its fresh
IDs even after uncertain create responses, and observes absence twice. Source
images remain as evidence, and no registry pruning occurs.

After byte equality, publish one owned application canary through the normal
candidate builder and start a real sandbox using the existing SDK. Verify its
recorded application smoke result, the canary's file bytes/modes/owners/link
targets/hardlink equivalence, and absence semantics for fallback cases. Always
delete only that owned sandbox in `finally`. This confirms the real publication
and runtime assembly route, which the read-only comparison intentionally does
not exercise. Preserve the existing filesystem format until these checks pass.

## Gate 2: comparable 48-request bursts

Use the original server-side contexts at
`/work/ucloud-sandboxes/build-load-20260929/contexts`. Do not regenerate locks,
resolve new base tags, change timestamps, or alter dependency graphs. The
archived `../build-load-2026-09-29/fixture-manifests.json` has SHA256
`45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7`.

`scripts/qualify_build_optimization.py` verifies all context bytes and complete
fixture metadata (including expected smoke results) against that frozen
inventory. It invokes the existing harness with 48 simultaneous requests:
three recipes × `app-change-5` through `app-change-20`, unique image IDs,
1,200-second per-request timeout, and exactly four builders. Its default only
verifies inputs and prints the proposed command. `--run` records the declared
wheel digest, actually imported **gateway** source hashes, context hashes,
harness hash, and UTC phase boundaries, then executes one phase.

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python \
  /work/ucloud-sandboxes/qualify_build_optimization.py \
  --source-root /work/ucloud-sandboxes/build-load-20260929 \
  --output-root /work/ucloud-sandboxes/build-optimization-20260929 \
  --fixture-manifests /work/ucloud-sandboxes/FROZEN-fixture-manifests.json \
  --harness /work/ucloud-sandboxes/live_build_load_benchmark.py \
  --harness-sha256 FROZEN-HARNESS-SHA256 \
  --phase b1-app-a --candidate B1-selective \
  --artifact-sha256 DEPLOYED-WHEEL-SHA256
```

Review the printed plan, then repeat with `--run`. Choose fresh phase names for
each execution; the wrapper never overwrites a phase or silently reuses image
IDs. It creates only an artifact-root symlink to the original contexts. It
does not prepare/retire nodes or deploy packages. The harness refuses unrelated
active builds/sandboxes, stale builder heartbeats, fewer/more than four ready
builders, and insufficient builder disk headroom. On wrapper timeout, inspect
the owned build IDs to completion; do not kill unrelated jobs or prune caches.

Use a separate owned SDK `prepare_id`, such as
`build-optimization-20260929`, with count four and a bounded TTL covering the
experiment. Release exactly that reservation afterward. Do not use the older
harness's hardcoded preparation ID to release someone else's reservation.

For causal attribution, use these release states when practical:

| Release | Selective EROFS | Gateway/node admission | Purpose |
| --- | --- | --- | --- |
| B0 | Existing Docker behavior | Existing four execution + four queue slots | Current baseline |
| B1 | Candidate | Existing admission | Isolate partial-materialization cost |
| B2 | Candidate | Four nonterminal/execution slots, zero extra queue | Isolate placement/admission cost |

Build each wheel from the same otherwise-current source; record its SHA256.
Keep four CCX33 builders, BuildKit version, max execution four, cache import
selection, cache export mode, GC policy, base digests, and request order fixed.
Verify source/bundle fingerprints on the actual builder nodes, not merely the
gateway's imported sources. Running builders do not adopt a new provisioning
bundle until replaced. Whole-fleet restart/cache loss during a version change
is a material confound and belongs in the receipt.

Run a matched warm primer under each version, and preserve per-build cache and
selective-path metrics. Replaying identical contexts after a previous phase
can make every EROFS group a full hit; in that case Gate 2 tests scheduling but
does not measure selective misses. Do not claim selective speedup from a
full-hit replay. Gate 1 provides controlled partial-miss evidence; if a new
end-to-end partial-miss series is required, freeze an additional set of real
application edits **once**, preserve manifests/locks, and use matched cohorts
with explicit cache-state reporting. Do not delete canonical registry groups.
ABBA order can expose warmup/drift, but cannot undo cache warming by itself.

The prior overload phase completed 48/48 in 172.2 seconds with client p95
162.5 seconds and recovered 133 submission 503s. That is historical context,
not a controlled baseline for a newly warmed fleet or changed release.

Collect gateway and every builder with `scripts/build_load_telemetry.py`
(`sample --duration 1800 --interval 2`, unique output/unit per host); include
registry volume bytes/await/queue, CPU/IO PSI, and builder execution/queue
timings. Use the existing `build_load_report.py` and
`build_load_host_analysis.py` over the raw receipts. Their disk busy-counter
anomaly exclusions still apply. Do not omit failed requests, submission waits,
or SDK retries from client latency.

Acceptance requires all 48 distinct build IDs terminal and successful,
48 durable build-history rows, and the existing three-recipe real-sandbox
smoke checks via `build_load_qualify_images.py --root ROOT smoke --phase PHASE`.
Report client p50/p95/max and batch duration alongside submit wait, node queue,
execution, environment phases, cached/missing/rebuilt groups, physical registry
IO, and fairness while peers have capacity. A 503 count alone is not a failure
or improvement. Do not add phase p95 values to estimate a total p95. Preserve
all errors and bound cleanup to owned sandboxes/preparations/artifacts.

## Rollout and rollback

The previous audited controller is
`../buildkit-cache-2026-09-29/deployment-controller.py`; its ROOT and rollback
backup are hardcoded to `buildkit-cache-optimization-20260929-r3`. It is a
reference implementation, **not the rollback target for this release**.
The controller staged beside this document must use a fresh release root and
capture the currently deployed config and gateway venv before changing them.

Preserve its wheel/dependency/entrypoint and native-bundle inventory checks,
receipt digest fence, idle build/sandbox/relay guard, owned atomic config write,
fresh-interpreter health checks, and automatic restoration on a failed health
check. Preserve the existing 64-GiB relay budget and BuildKit cache settings.
Verify the new backup's hashes before applying. Retire only idle old builders
and provision candidate builders from the new bundle; never force-kill work.

Stage → verify the concrete receipt → apply the authorized candidate during
the idle guard → health/canary → bounded burst → preserve receipts. On failed
health, wrong EROFS bytes, incorrect sandbox semantics, or unacceptable client
regression, use this release's verified rollback snapshot; replace candidate
builders only after their owned builds drain. No rollback should invoke global
registry GC, clear shared caches, or restore the older r3 config by accident.

## Local preparation checks

```sh
.venv/bin/python -m unittest \
  tests.test_qualify_selective_environment tests.test_qualify_build_optimization \
  tests.test_qualify_selective_semantics tests.test_qualify_selective_runtime
.venv/bin/ruff check scripts/qualify_selective_environment.py \
  scripts/qualify_build_optimization.py tests/test_qualify_selective_environment.py \
  tests/test_qualify_build_optimization.py scripts/qualify_selective_semantics.py \
  scripts/qualify_selective_runtime.py tests/test_qualify_selective_semantics.py \
  tests/test_qualify_selective_runtime.py
```

These tests exercise rejection of registry writes, independent byte validation
of captured signed components, and detection of changed frozen context bytes
or expected runtime results. They also verify the guest checker against real
local files/hardlinks/symlinks, detect corrupted contents, validate explicit
selection from incomplete receipts, and fence foreign sandbox IDs during
cleanup. They do not substitute for the live canary gates.
