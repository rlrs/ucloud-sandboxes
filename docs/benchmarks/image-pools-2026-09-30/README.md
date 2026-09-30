# Image precomputation at portfolio scale, 2026-09-30

The production preparation workflow now separates complete reusable image preparations from shared foundations and unresolved task work. This is not a qualification of an actual training selection: that selection was unavailable. BIRD and NeMo were explicitly deferred. Train/heldout membership was not inferred, and Bash/OpenCode/Pi variants were not counted as separate task filesystems.

The source inventory contains 35,984 distinct references across the SWE and evaluation/base pools. Separate full recipe inventories contain 36,884 OpenSWE and 14,600 TMax recipes. The implementation selects high-reuse images, samples distinct repositories when that metadata is available, pins linux/amd64 manifests, validates signed artifacts and bounded sandboxes, and retains complete registry closures.

## Initial completed shared foundations

- All 11 OpenSWE Python foundations are retained. Exact rewrites matched 36,385 of 36,884 source recipes (98.6%). Repository checkout and project-specific installation remain.
- One explicit TMax scientific foundation plus 25 literal dependency-prefix foundations match 9,321 of 14,600 recipes (63.8%). The additional 25 cover 4,187 formerly monolithic installers.
- All 37 foundations passed registry-based resume without new builds. Their union of EROFS components occupies 19,641,401,344 bytes. This excludes OCI/cache storage and builder scratch space.
- A stricter input audit classifies 155 rewritten OpenSWE recipes as unknown because their remaining Dockerfile forms or external stage inputs are not supported by the audit. An exact foundation replacement is not proof that every remaining input is prepared.

## Real task comparison

Two TMax task contexts were built using the largest inline dependency foundation. For the first, an original-recipe baseline and a factored build produced identical checked task files, package versions and selected environment values. Checks covered regular nonsymlink files smaller than 1 MiB under `/app` and `/home/user`, file metadata, `pip freeze`, and selected environment variables. They were not full filesystem equivalence or oracle grading.

| Measurement | Original task A | Factored task A | Factored task B |
| --- | ---: | ---: | ---: |
| New EROFS bytes | 362,860,544 | 24,576 | 16,384 |
| Reused foundation bytes | 0 | 362,840,064 | 362,840,064 |
| Docker build/push | 57.935 s | 16.883 s | 7.448 s |
| Filesystem preparation | 17.472 s | 4.230 s | 1.145 s |
| Finishing queue | 0.004 s | 79.172 s | 8.144 s |
| Backend end-to-end time | 75.583 s | 100.452 s | 16.939 s |

The factored build removed almost all repeated filesystem work. It did **not** always improve wall time: bulk conversions delayed task A by 79 seconds. These are individual observations under changing concurrent load, not an isolated throughput comparison. The baseline's later receipt-revalidation wall time is deliberately excluded.

## Source pools and verification

Machine-readable, aggregate results are in [results.json](results.json). The completed initial batches prepared 181 distinct sources. The expanded queue contains all 35,984 inventoried source references and a separate 2,786-foundation TMax plan matching 9,383 inline recipes, and 5,804 Terminal dependency prefixes matching 12,545 raw recipes; these are candidates, not completed builds. Admission limits actual preparation to the registry storage budget. A source is counted ready only after its artifact and sandbox check; deferred and failed candidates are not counted as prepared. Dataset row counts are declared upstream uses, not the unknown training sampler.

SWE-smith preparations fetch repository branches and install ripgrep. They use explicit prepared image IDs; they do not replace original upstream digest aliases. Only the exact corresponding preparation recipe can skip its whole build. Other faithful imports can activate ordinary upstream-reference aliases. Existing different aliases remain unchanged, and qualification falls back to an explicit prepared ID when necessary.

Preparation has persistent accepted-build journals, global per-image claims on the gateway, durable publication records, and catalog-based registry recovery. A real resume initially failed when a builder's history disappeared; the retained artifact was recovered without rebuilding. Existing artifacts are verified before old job history is consulted. Fleet inventory is read once per coordinator rather than once per source.

The expanded campaign is configured for 16 source-preparation workers, eight TMax foundation workers and four Terminal foundation workers, an eight-builder ceiling, a 1,800 GiB observed-growth allowance per coordinator group, a 500 GiB free-space admission reserve, and a 5 GiB per-source compressed-size cap. The production registry ceiling is 3 TB. All three coordinator groups observe total registry growth; their in-flight reservations are local to each group, so the substantial free-space reserve remains necessary. Those are scheduling estimates, not hard decompression quotas. Other registry use counts against the observed growth allowance. Very large inputs remain explicit gaps, including SWE-smith MONAI (17,870,919,470 compressed bytes) and FVCore (7,526,569,843 compressed bytes).

The pre-migration qualification passed all 181 ready sources with zero new imports. Create latency was p50 2.638 s, p95 7.621 s, and maximum 97.974 s. The initial requests included worker provisioning from a fleet scaled to zero; autoscaler logs confirm a new worker was requested. The test ran during the volume copy and is not an isolated steady-state performance result.

Qualification exercises creates, exec/file operations and deletion, and checks that requests do not submit new import builds. It uses eight concurrent sandboxes, root execution matching the pinned training integration, finite TTLs, and image mount/exec checks. It is not a 500-agent load test or qualification of every task's build/grading commands.

## Storage migration, larger queues and recovery

The active registry is now 3,000 provider GB. The old 8,000 GB volume was deleted after the user explicitly authorized retirement, a full checksum copy, reversible cutover, and 181/181 post-cutover image checks. Post-cutover create latency was p50 2.541 s, p95 5.938 s and maximum 94.840 s, again including worker provisioning from zero. There were no new import builds. Maintenance timers and gateway health were verified after migration.

At the latest aggregate snapshot, the three running queues contained 281 ready source images, 81 ready TMax inline prefixes covering 5,300 raw recipes, and 57 ready Terminal prefixes covering 3,939 raw recipes. Eleven source failures and three Terminal failures/deferred entries remained explicit gaps; they did not enter the ready counts. Counts are snapshots of an ongoing campaign, not the final preparation result. The signed EROFS union for the Terminal prefixes was 5,288,435,712 bytes; this excludes OCI and cache storage.

A real Terminal task built from the original and factored recipes passed comparisons of its pinned input file bytes/metadata, pip package versions, selected environment, working directory and pandas version. Both variants reused 157,233,152 bytes of existing EROFS components and added only 4,096 bytes. This case demonstrates existing component sharing as well as a small task delta; it does not demonstrate additional storage savings from factoring. Backend build/push was 26.316 s original versus 33.962 s factored, while finishing wait was 130.822 s versus 0.001 s. Their different wall times are dominated by changing queue conditions, not proof of a build-speed improvement.

The [portable campaign](../../../image-campaigns/2026-09-30/README.md) is committed separately from the registry. All 8,602 foundation contexts materialized and validated offline from its 1.3 MB input bundle. Fresh recovery identities rebuilt a pinned Microsoft source image and the real pandas Terminal foundation and passed sandbox checks. An Ubuntu source check encountered Docker Hub 429 throttling. These checks exercised fresh work directories and stale-identity avoidance on the existing registry; they did not delete production data or simulate loss of every builder cache. The 38 focused unit tests passed.

Recovery commands prioritize shared foundations, reserve 600 GiB of the total growth allowance from full source imports, and retain the 3 TB ceiling. Upstream packages are not universally pinned; recipe recovery does not promise byte-identical artifact restoration. Restore from new validated catalogs and rewrite the actual task index; existing aliases are preserved and can still point at lost artifacts after a real volume loss.

## What this does not establish

A shared base is not a completed task image. Terminal-Lego, TBLite and Senior SWE base coverage still leaves their task contexts and dependency commands. The larger SWE pools contain many unique images; sampling a repository does not establish readiness of all its tags. Flattened upstream filesystems may share little at the layer level. BuildKit cache and EROFS component reuse solve different parts of preparation and both still consume storage.

Before a training run, rewrite its actual recipe index and run `plan_image_pool.py audit-index` with explicit live-build and cold-build allowances. Unknown prepared IDs fail the audit. Qualify the allowed live recipes and verify live artifact availability; the audit itself is an offline receipt check. The current training index was not available and has not been switched to these foundation/prepared-image references. No claim is made that the next arbitrary training selection requires only a few builds.

## Reproduction and inputs

See [image-pools.md](../../image-pools.md) and [image-foundations.md](../../image-foundations.md) for planning, bounded preparation, rewriting and auditing commands. Focused tests exercise conservative script factoring, original-index preservation, stage handling, exact enriched-recipe substitution, digest/platform validation, storage accounting, alias publication, and recovery after builder history loss.

Source definitions: research-environments `c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92`, verifiers-ucloud `7856cc18faceae31aa533605ea812d81b92a85ea`. TMax source: prime-tasks `8b38d35b53271a5f955dfc5dd8197d562cebf46e`. OpenSWE source: `a8db93af5335df2c8baac0cd1ff367e4d475d3d7`. Evaluation recipes were pinned from their upstream defaults; these are not asserted to be the user's exact heldout revisions. No credentials, private fleet addresses, complete production receipts or task answers are published here.
