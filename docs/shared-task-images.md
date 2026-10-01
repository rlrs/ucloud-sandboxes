# Compact preparation of task images

Task-specific upstream images count as required bases. The acceptance target is every inventoried task base prepared, with only separately measured small steps left live. `plan_image_pool.py report --require-all-task-bases` enforces that distinction; all 94 generic bases alone cannot pass. At the completed September 30 pilot snapshot, coverage was **742/35,984 source references**, not 100%. BIRD/NeMo and the unavailable actual training selection remain outside this inventory.

The new preparer retains a verified shared filesystem plus a delta for flattened ScaleSWE images. It downloads and authenticates the original source as temporary input, compares all tar entries against a retained same-project anchor, and publishes only the anchor plus delta. It preserves source runtime configuration. BuildKit uses these OCI layers; the existing EROFS builder reuses the anchor's signed component. There is no second retained full copy of the target OCI layer in this preparation path.

This is an offline preparation tool, not an automatic rewrite in the request path. After qualification it inserts ordinary upstream-name and digest aliases. Existing different aliases are preserved. A production check created a new task image through its original public name in 1.146 seconds without an import build; no SDK update is required. That single check is not a load-test latency guarantee.

## Measured pilot

[Evidence](benchmarks/image-pools-2026-09-30/shared-task-preparation.json) records 48/48 successful preparations over 12 project anchors. Their compressed deltas total 59,254,673 bytes; their original compressed layers total 26,474,609,014 bytes. Observed total registry growth was 159,645,696 bytes. This excludes already-retained anchor costs and includes concurrent registry activity. Some original images already share EROFS blobs, so avoiding their full OCI layers is a separate saving from filesystem-component sharing. No original images were deleted.

There were 36 full source filesystem scans and 12 checks using a prior exact-filesystem certificate plus a fresh sandbox mount/exec. One initial verification failure involved Helm files under `/tmp`; the deployed sandbox always replaces `/tmp` with tmpfs. The verifier now follows that runtime contract, and the image passed on retry. Failed verification did not register its source aliases.

The expanded queue contains 8,565 candidates over 118 retained anchors. The current coordinator runs four children under 300% gateway CPU and 6 GiB memory limits, with a **persisted 64 GiB growth allowance** from the original baseline and **500 GiB free-space floor**. The earlier two-worker coordinator and waiting continuation were drained/stopped before this replacement started. The registry was subsequently expanded to 3,500 provider GB for additional evaluation coverage; the original accounting baseline was preserved. Storage-deferred rows are reconsidered against current admission on resume; the floor is unchanged. Candidates are not coverage. Storage admission includes a 2 GiB reservation per active child. New projects can have much larger deltas than the initial pilot; actual growth determines how far the batch proceeds.

## Qualification and failure behavior

- Authenticate compressed source/config digests, signed prepared artifacts, source-layer bindings and target runtime configuration before aliases become usable.
- Reject unsupported tar entries, unsafe paths, invalid hardlinks, implicit ancestors, non-amd64/Linux sources and `ONBUILD` recipes. Flat sources use the existing direct path. Multi-layer sources require protected offline exports and the additional qualification described below.
- Bound compressed inputs to 2 GiB each, indexes to 200,000 entries/64 GiB regular-file bytes, and changed regular-file data to 256 MiB. Large changes are deferred.
- Compare actual sandbox contents, metadata, links and xattrs against the authenticated source. Follow existing EROFS timestamp normalization (`mkfs -T 0`) and runtime mounts/injected files; this does not promise preservation of Docker's original timestamps or files hidden by sandbox mounts.
- Reuse a full scan for at most 24 hours only when expected source contents, physical component identities/order/ranges/formats, runtime config, scanner contract and worker bundle match. Every image still gets a fresh sandbox mount/exec. Source provenance remains authenticated separately.
- Preserve accepted and successful build receipts. A successful artifact can be rechecked after its builder scales down and job history disappears. Cleanup removes only this locked job's marked, recognized temporary input files.
- Respect the shared public-registry cooldown outside child admission. Source failures, unsupported cases and storage deferrals never count ready. Storage deferrals are reconsidered on resume without resetting the accounting baseline. `--retry-failed` archives prior failed journals and preserves the original budget baseline.

The scanner requires Python 3 in the source image. Qualification establishes the platform filesystem contract, not execution of every benchmark test or grading command. Full upstream downloads and indexing remain offline costs; the accepted backend build duration alone understates preparation wall time.

## Run and resume

Use a compatible package containing `oci_flat_delta` and `flat_image_qualification`, the repository scripts together, and the deployment service account. The production preparer uses an isolated package directory; the gateway, worker runtime and SDK were not replaced. The qualified wheel SHA-256 is `83ace15dcc74c487967c8b5f6a2317db85f7874fd069c71961c791c6b32e0cb0`.

```sh
python scripts/plan_shared_task_pool.py \
  --inventory /data/source-inventory.json \
  --catalog /data/original-source-catalog.json \
  --output /data/shared-tasks/plan.json

python scripts/prepare_shared_task_pool.py \
  --root /data/shared-tasks --gateway https://sandbox.example \
  --sdk-wheel /data/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl \
  --workers 2 --limit 8565 --growth-limit-gib 16 --free-floor-gib 500
```

Run under a durable supervisor with the resource limits above. Resume with the same root and budget file; do not reset the baseline to make deferred work admissible. `results/` contains per-source outcomes, `catalog.json` and `progress.json` checkpoint every four completions, and each child retains its source resolution before building. Plans include public anchor provenance as well as currently prepared private references. An inventory `pinned_source` is passed through and enforced during recovery.

## Recovery

The [portable inputs](../image-campaigns/2026-09-30/README.md) preserve 743 public source pins and all 8,602 foundation contexts independently of the registry volume. `shared-anchors.json` contains the 118 original public anchor inputs without private references or completed-build state.

After volume loss, use a fresh recovery generation. Restore the required anchors from that public plan with the normal source preparer, adding a fresh `rebuild_generation` to its plan. Materialize the portable bundle for pinned source inventory, then regenerate the shared-task plan from the **new** anchor catalog. This derives new private references and preparation identities. Never reuse stale successful catalogs, old private anchors or a previous shared-task work directory after losing their registry contents. Existing aliases are insert-only; use explicit restored references and rewrite/audit the task index as described in the campaign guide.

Refresh the off-host inputs as more images complete:

```sh
python scripts/image_campaign.py refresh \
  --bundle image-campaigns/2026-09-30/inputs-shared-pilot.json.gz \
  --catalog /data/shared-tasks/catalog.json \
  --output /data/inputs-next.json.gz
```

Refresh preserves every earlier pin and foundation byte, rejects conflicting source pins and writes no success receipts or credentials. Preserve the resulting bundle off-host. Public images can disappear; recipe recovery is not a registry-blob backup or a guarantee of a byte-identical rebuild.

A larger expanded-queue delta was profiled separately: 253,924,323 changed logical bytes were new files, 1,933,116 were timestamp-only changes and 332,650 were changed file contents. Its large size is therefore not explained by redundant timestamp copying. These are logical bytes, not compressed savings.

## Qualified multi-layer preparation (October 1)

The [measured results](benchmarks/image-pools-2026-09-30/layered-task-preparation-20261001.json) include 14 ordinary imports across seven families, an existing-image sharing proof, and two fresh R2E-Gym preparations. The fresh sources required **37.7 MB and 42.1 MB** compressed deltas, plus **68.4 MB and 77.9 MB** of new EROFS data; each reused seven existing components. Their original compressed sources were about 448.6 MB each. Existing anchor costs are excluded. The earlier 507.4 MB source / 168.2 MB delta proof used an already imported target and did **not** reclaim its original blobs.

The existing-target proof and three fresh layered preparations passed independent full sandbox filesystem scans. The third fresh source also exercised the final disk-bounded unpacker: a 171.4 MB delta replaced a 510.6 MB source, and its temporary filesystem was removed. Fresh-source requests through the ordinary public names created sandboxes in 1.34 and 1.47 seconds without an import build. These are small canaries, not a workload latency guarantee. The full-source checker now accounts explicitly for the runtime-created `/workspace` directory only when absent from the source; existing workspace contents, ownership, mode and timestamps remain checked. This advances the certificate contract to version 3 and forces old certificates to be requalified when using the new package.

`export_oci_filesystem.py` verifies the pinned source manifest/config and compressed blobs, then invokes a SHA-256-pinned distro `umoci` binary inside a minimal chroot with no network, no image execution, and a read-only root. Unpacking writes to a dedicated, temporary **16 GiB ext4 filesystem**, mounted `nodev,nosuid,noexec`; decompression cannot consume unbounded gateway root space. Source inputs are at most 5 GiB compressed / 64 layers. Export preserves numeric ownership, modes, links, timestamps and xattrs, following the existing EROFS excluded runtime trees. It removes the unpacked filesystem and source-blob scratch after a successful export. A failed unmount preserves its backing file for investigation instead of deleting a mounted filesystem.

Example workflow, using new directories and immutable references:

```sh
# Run exports as root. Use an authenticated resolved.json for the public target.
python scripts/export_oci_filesystem.py --reference "$ANCHOR_PIN" \
  --root /data/exports/anchor --umoci /opt/umoci --umoci-sha256 "$UMOCI_SHA256"
python scripts/export_oci_filesystem.py --reference "$SOURCE_PIN" \
  --resolved /data/source-receipt.json --root /data/exports/target \
  --umoci /opt/umoci --umoci-sha256 "$UMOCI_SHA256"
```

Keep the export parent, child directories, archives and reports owned by root and not group/other writable; grant the service account read/traverse access. Run `prepare_shared_task_image.py` as that account with its usual arguments and `--filesystem-exports /data/exports`. Exports are bound to the authenticated source reference, config, ordered layer descriptors and diff-ID chain. The preparation identity also includes both export digests. Only the new delta is uploaded; all anchor blobs are mounted from the retained registry. Full filesystem qualification and persistent retention precede insert-only public alias registration.

The deployed experimental package is isolated from the running flat queue and runtime services. Its distro unpacker is Ubuntu `umoci 0.4.7+ds-4`, binary SHA-256 `4796f71e1dc93959b5d5c33066ee5a51542a16879df92a21fc0d6227a62d89d6`; the package was extracted, not installed. The default changed-file bound remains 256 MiB; the individually reviewed layered proofs used 512 MiB. This path is qualified for these cases, not yet an unattended whole-corpus layered coordinator. Export archives also need a bounded retention policy before large-scale reuse.

## Storage expansion and upstream limits

The volume increased from 3,000 to **3,500 provider GB**, within the user's conditional 4,000 GB ceiling. Ninety pinned evaluation candidates were missing 42.134 GB of distinct OCI blobs. The 14-image batch added 3.069 GB of OCI data and 4.989 GB of EROFS, a 1.63 ratio; extrapolating that selected sample gives roughly 111 GB of additional data, beyond the approximately 65 GB then available above the 500 GiB reserve. This justified additional capacity, but does not establish a whole-corpus storage forecast. No automatic resize is configured.

Confirmed Docker Hub HTTP 429 responses now pause unresolved work through the shared cooldown. The source resolver records HTTP status codes so throttling is distinguishable from 502/503/504 failures. Ninety pinned evaluation bases and 24 pinned training bases can build without another manifest lookup. The remaining 643 evaluation candidates are queued for actual preparation under that cooldown. They are not counted as ready. Large unsupported inputs remain explicit deferrals.

The new metadata inventory reports the distinct missing OCI union as a lower bound and writes source receipts before continuing. It is not an EROFS capacity estimate or a readiness catalog. Metadata-only probing was stopped once its saved receipts were handed to preparation; this avoids competing discovery and preparation requests for the same sources.

The two pinned source batches were resumed at four workers each using their original journals, accepted build IDs and storage baselines. Each coordinator retains a 100% gateway CPU cap and 2 GiB memory limit. The provider builder ceiling stays at eight; autoscaling determines actual node count. Five additional pinned R2E sources use the bounded compact path serially under a separate 100% CPU / 3 GiB cap. These queued sources remain outside ready coverage until qualification succeeds.

The initial 64 GiB compact budget eventually paused the ScaleSWE queue at 116 ready, because it conservatively counts all registry growth, including the simultaneous evaluation imports. A supervised continuation now waits for the two pinned batches to drain, then resumes the same 8,565-source plan with a 384 GiB allowance from the **unchanged** original 2,407,102,504,960-byte baseline. Its CPU cap is 150% and the free floor remains 500 GiB. This uses the measured 3.5 TB capacity while avoiding another CPU-heavy queue during the pinned batches. Storage-deferred rows are retried; the one changed-file-bound deferral remains explicit.


## Cross-project sharing and recovery (October 1)

An existing Click anchor also qualified an Optimizely task from a previously
unrepresented project: 444.4 MB of original compressed image became a 10.6 MB
OCI delta plus 15.8 MB of new EROFS data. The existing 772.3 MB anchor is shared;
it is not charged anew to every task. All 41,669 source filesystem entries passed
comparison. This is a measured canary, not a whole-corpus compression estimate.

`plan_shared_task_pool.py --fallback-anchor-source <original-public-source>`
explicitly enables cross-project candidates when no same-project anchor exists.
The fallback must be a retained original image within the anchor bound; derived
images are not silently substituted. Repeatable `--exclude-plan` avoids overlapping
queues. Each candidate still requires full filesystem qualification before alias
publication. The current additional plan has 8,494 candidates across 1,007 source
projects, separate from the existing 8,565-source queue. Its first 48 candidates
are a bounded pilot; candidates are not ready coverage.

Docker Hub throttling is scoped carefully. A confirmed pull-quota error can
leave a repository with observed unlimited pull capacity usable. An authenticated
anonymous HEAD must return 200, an image digest, and no rate-limit headers before
that exact repository may proceed. The shared quota state remains in force for
other repositories, and all requests retain pacing. Generic 429 and 5xx responses
never bypass backoff. Proofs expire after an hour and are revoked on a failed
request. Temporary quota deferrals are retried; storage/size deferrals and hard
failures remain explicit. `progress.json` distinguishes queued, active and completed
work from historic result journals.

All 90 pinned evaluation candidates and all 24 additional pinned training inputs
are prepared, including the three 6.1–8.8 GB compressed Terminal-Bench sources.
The original 111 smaller preparations passed public-name create/exec checks with
eight concurrent checks, zero new import builds, create p50 2.23 s / p95 6.29 s.
Five more fresh layered R2E images passed full source comparisons. These are cache
qualification results, not a guarantee for a 500-sandbox training workload.

At 00:08:23 UTC, scheduled garbage collection stopped the registry for 80 seconds
and reclaimed only 534,693 bytes. Preparation and alias checks during that window
failed. The deployed fix requires an explicit maintenance-window flag before
physical collection or pressure eviction can interrupt serving. Automatic timers
now report deferred maintenance; online reference pruning and disk admission
remain. See [registry maintenance](managed-registry.md#blob-sweep). Interrupted
campaigns retain their source pins, storage baselines and accepted-build journals.
`--retry-failed` on the shared coordinator permits one new build after a confirmed
terminal failure, preserving the old receipt; a wait timeout never duplicates a
potentially running build.

The 00:10 UTC catalog snapshot had 1,164 of 35,984 source references ready,
including all 94 generic bases and all 89 Terminal-Bench 2 sources. Overall task
coverage is still incomplete. The portable inputs now preserve 1,204 immutable
source pins; the separate metadata snapshot has 922 authenticated manifest/config
receipts. Neither archive claims blob backup or completed coverage.

## Compact project bases

`--allow-compact-anchors` permits a fully source-qualified compact ScaleSWE image
as an anchor within its own project. Original anchors remain the default and the
explicit cross-project fallback must still be an original image. Compact planning
chooses the closest available PR number within a project, preferring an original
anchor on ties. PR proximity is a deterministic heuristic, not proof of similarity;
full source qualification still decides correctness. Existing job assignments are
preserved when a campaign resumes.

The preparer accepts `--anchor-filesystem-source` only as an immutable public pin.
It authenticates that source's original flat manifest/config/layer and indexes it
as the comparison input, while retaining the **actual compact anchor's OCI chain**.
Only the target delta is published. The final sandbox is compared against the full
original target filesystem before aliases are registered. This avoids keeping a
second complete flattened source for every compact project base. Compact anchor
chains are bounded at eight layers; source bounds and scratch reserves still apply.

Three cases passed: an existing Optimizely source (PR102, not new coverage), a new
Cookiecutter source (PR1496), and a new Optimizely source (PR128). The fresh PR128
used **449 compressed bytes and a 4,096-byte EROFS component**, reusing both anchor
components. Cookiecutter used 13.65 MB of OCI plus 19.27 MB EROFS. All source entries
were checked. The remaining 23 fresh public-alias checks passed at four concurrent
checks with no import builds, create p50 1.26 s / p95 1.53 s. The earlier 33 checks
also passed; their report was recovered from exact journal records after its output
file write failed because the parent directory was root-only.

`prepare_project_image_campaign.py` makes this a supervised two-stage workflow:

```sh
python scripts/prepare_project_image_campaign.py \
  --root /data/project-campaign --inventory /data/inventory.json \
  --catalog /data/immutable-ready-catalog.json \
  --exclude-plan /data/other-active-queue.json \
  --fallback-anchor-source aweaiteam/scaleswe:pallets_click_pr1000 \
  --gateway https://sandbox.example --sdk-wheel /data/sdk.whl \
  --workers 4 --growth-limit-gib 256 --free-floor-gib 500
```

First it attempts one compact base per project without a qualified anchor. Then it
plans remaining tasks against the qualified project bases. Both stages inherit
one immutable observed-growth baseline; entering the second stage does not reset
the budget. Inputs are checksummed, assignments survive resumption, and additional
qualified projects can append previously unassigned tasks. Failed/deferred seeds
remain uncovered. The deployed seed stage contains **996 projects**, excluding the
existing 8,565-source queue and the separate pilot/canaries. It has a 200% CPU cap,
4 GiB memory cap and low CPU scheduling weight. The first large candidates exceeded
size bounds; they are not counted as ready. `cost.json` records compressed input
size and, after indexing, changed-file bytes so later capacity decisions are based
on measurements.

Registry unavailability now pauses new source work and shared-child admission.
A five-second cached health check prevents an outage from immediately consuming
thousands of pending candidates as failures. It cannot prevent an already-running
request from failing; explicit terminal-failure retry preserves its journal.

A three-delta compression benchmark used 20.17 CPU seconds at gzip level 9 versus
6.73 at level 6, with compressed bytes increasing from 102,329,326 to 102,773,018
(**0.43%**). New project jobs use level 6. Compression is part of the preparation
identity, and existing jobs preserve their recorded setting, including legacy
level 9. A fresh level-6 source passed full equivalence and public-alias checks.

The existing same-project queue now overlaps six preparations under a 300% CPU /
6 GiB limit; evaluation overlaps two under 100% / 2 GiB. All three campaigns have
low CPU scheduling weight. Builder provisioning remains bounded by the existing
ceiling of eight. These overlap settings do not add host capacity. The registry
remains at 3,500 provider GB with a 500 GiB free-space floor; no further expansion
has been justified or performed.

## Measured seed limits and serial layered queue (October 1 01:20 UTC)

Comparing two oversized seeds against all 118 cached original anchor indexes
saved only 871,199 changed bytes in each case: Pipecat Flows remained 713,025,793
bytes and DVC Render 321,407,041 bytes. `score_shared_task_anchors.py` performs
these comparisons from authenticated, bounded metadata without downloading source
blobs or publishing images. Source index recording is optional and enabled for
project seeds; its compressed/uncompressed bounds are 32/128 MiB. Scores estimate
changed logical bytes, not compressed storage or readiness.

A larger fakeredis project seed passed full source equivalence: 403,655,716 changed
bytes became a 214,245,450-byte compressed delta, versus 649,070,428 bytes for the
original source. The inventory contains 31 tasks for that project; the other 30
are candidates, not qualified coverage. This supports a separate one-time seed
limit rather than relaxing every task's bound. The project campaign now uses
`--seed-max-delta-mib 1024`, three concurrent preparations under the same 200% CPU /
4 GiB cap, and its unchanged 256 GiB registry-growth baseline. Its task phase
retains 256 MiB deltas and admits qualified project anchors up to 2 GiB of EROFS.
A measured delta deferral is retried only if it fits the explicitly requested
limit. The larger setting reserves 3 GiB per active seed before admission.

`prepare_layered_task_pool.py` turns the successful isolated-export path into a
serial queue against one pinned, exported project anchor. A root-owned immutable
`plan.json` contains `schema: 1`, `anchor`, `anchor_source`, and inventory `images`.
The coordinator checks disk and registry availability before each job, observes
the shared public quota as the service account, and preserves authenticated source
receipts and accepted build journals. Exporting uses the existing bounded,
non-executing unpacker; preparation and full sandbox qualification run as the
service account. Temporary target exports are removed after each attempt. Cleanup
refuses any tree containing an active mount and never follows symlinks out of it.
The shared anchor export remains available for the queue.

```sh
sudo -E python scripts/prepare_layered_task_pool.py \
  --root /data/layered-project --anchor-exports /data/protected-anchor-export \
  --umoci /data/umoci --umoci-sha256 UNPACKER_SHA256 \
  --gateway https://sandbox.example --sdk-wheel /data/sdk.whl \
  --growth-limit-gib 64 --free-floor-gib 500 --max-delta-mib 512
```

The first deployed queue has 229 remaining aiohttp R2E images, one at a time under
100% coordinator CPU / 3 GiB memory, plus the separately bounded unpacker. Its
64 GiB growth budget includes other concurrent registry growth. It stops admission
at a budget/reserve boundary, preserving pending work. Unsupported sources remain
explicit outcomes; this queue does not imply coverage of the other R2E projects or
of the entire layered corpus. A fresh registry recovery needs newly built anchors,
new exports and a new plan; old ready receipts are not recovery inputs.

At 01:13 UTC, the aggregate inventory had 1,539/35,984 source references prepared,
including 94/94 generic bases and 89/89 Terminal-Bench 2 sources. Full task-base
coverage remains incomplete. The nested project seed catalog is included in this
snapshot. Saved source receipts and updated pins are copied off the server and
committed separately from disposable private runtime catalogs.

The first fresh queued layered source (`aiohttp_final:3d41df0...`) passed full
filesystem qualification, using an 86,386,245-byte delta in place of a
507,437,423-byte original. Its temporary export was removed and the next source
started. Public-name requests for this image and the larger fakeredis seed both
passed, creating sandboxes in **1.80 and 1.03 seconds** respectively with no new
import builds. These two checks validate cache routing, not high-concurrency
latency. The [request results](benchmarks/image-pools-2026-09-30/cache-path-qualification-20261001.json)
are retained. The updated path passed 76 targeted tests; all five deployed
preparation/planning helper hashes matched commit `f1e3ce3`.
