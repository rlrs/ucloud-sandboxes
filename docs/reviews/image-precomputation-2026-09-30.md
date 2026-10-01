# Precomputing SWE and terminal environments

The highest-value change is to prepare and retain **launch-ready environments before training**, while factoring repeated dependencies into immutable shared bases and toolkits. Thousands of task manifests are inexpensive when they reference shared components; thousands of independent multi-gigabyte root filesystems are not.

There is an important limit to v0.7.0 sharing: all nine Scale-SWE images sampled here are single-layer, flattened filesystems with different digests. Three Monty images alone contain 26.22 GB of distinct compressed OCI data. Warming BuildKit or increasing its cache cannot make those images share their files. These require advance import/conversion, better upstream layering, or a separate filesystem deduplication design.

This is an analysis and implementation plan. No production changes, image builds, or image-layer downloads were performed for this study.

The user subsequently supplied the complete environment-family list: 14 training entries and 14 evaluation entries, representing 20 families after grouping heldout/dev variants. BIRD and the four NeMo families are explicitly deferred from immediate work. The expanded scope and evaluation findings are recorded below and in [environment-portfolio.json](../benchmarks/image-precomputation-2026-09-30/environment-portfolio.json).

## Evidence and scope

Inspected the exact requested revisions:

- [research-environments c7ea0d7](https://github.com/rlrs/research-environments/tree/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92).
- [verifiers-ucloud 7856cc1](https://github.com/rlrs/verifiers-ucloud/tree/7856cc18faceae31aa533605ea812d81b92a85ea).
- Backend source at `02811a724241d4dabcc9d94dcba6f3da620f6fdb`.

The study covers 122,681 rows projected from 37 SWE Parquet files, all 36,884 OpenSWE OSS recipe rows, 13,825 Terminal-Lego Dockerfiles/verifier scripts, and 14,600 TMax Dockerfiles/install scripts. The complete pinned OpenSWE JSONL was streamed and its SHA256 verified against the dataset's LFS metadata; unrelated task content was not retained. Registry inspection fetched manifests and configs for 27 images, without filesystem blobs.

The actual training `images.sqlite` and selection are unavailable. Counts below describe upstream pools, before task admission filters, exclusions, or sampling. OpenSWE and TMax use the recipe source's explicit pins; other dataset revisions were resolved during this study. Every revision and method is recorded in [inventory-summary.json](../benchmarks/image-precomputation-2026-09-30/inventory-summary.json). The registry sample is deliberately small and stratified, not a statistical estimate of whole-corpus storage or speed.

| Family | Inspected tasks/recipes | Distinct image references | Best precomputation unit |
|---|---:|---:|---|
| SWE-Smith, eight languages | 88,130 | 450 | Repository environment, including required task refs and dependency warm-up |
| OpenSWE OSS | 36,884 | 36,884 | Python foundation → dependency environment → exact repository revision |
| Scale-SWE | 17,202 | 17,202 | Prepared immutable image initially; investigate shared filesystem contents separately |
| R2E-Gym verified subset | 4,522 | 4,522 | Repository/toolchain/dependency version, after measuring actual overlap |
| Multi-SWE verified | 2,232 | 2,232 | Existing shared image prefixes plus per-instance preparation |
| SWE-Lego verified | 4,323 | 4,323 | Dependency environment and task image; broad repository diversity |
| SWE-rebench V2 verified | 6,272 | 6,272 | Same, resolving platform aliases to public image digests |
| Terminal-Lego | 13,825 Dockerfiles | Task-specific recipes | Shared verifier runtime and selected base/dependency stages |
| TMax | 14,600 Dockerfiles | Task-specific recipes | Two existing heavy-base scripts, then common initial dependency prefixes |

The pinned OpenSWE taskset contains a comment mentioning 27,188 aliases; the actual pinned OSS file has **36,884 distinct aliases**, with no collisions after the taskset's name mapping. Do not size the preparation queue from that comment.

Other adapters were traced but their task pools were not enumerated in the initial study: SWE-bench Verified/Multilingual resolve prebuilt images from task Dockerfiles; Pro uses `jefzda/sweap-images`; OpenThoughts TBLite uses per-task image aliases; Terminal-Bench 2 uses Harbor task configuration. Their coverage and storage are not included in this table. Pro is absent from the user's subsequent scope and is not an immediate preparation target. Senior SWE was subsequently enumerated as described below.

## Where precomputation has the most leverage

### SWE-Smith: prepare hundreds of environments, not tens of thousands of tasks

Python alone has 50,908 task rows but 131 image references. Across all eight languages there are 450 references. Task setup checks out an instance branch inside a repository environment. Preserve that behavior and precompute each distinct environment once. The [450 preparation candidates](../benchmarks/image-precomputation-2026-09-30/swesmith-preparation-candidates.json) are saved in descending upstream task-count order; these are source references requiring digest resolution, not already-prepared artifacts.

The index currently adds `git fetch` and ripgrep to the upstream image. Freeze the required refs and commits in a versioned preparation manifest instead of depending on whatever a mutable all-refs fetch returns later. The existing `ucloud_warm_swesmith.py` provides a starting point for warming dependency caches from a baseline worktree. Keep grader-only tests out of the shared agent environment. Retain the resulting EROFS components, not just a BuildKit cache entry.

### TMax: make the existing dependency boundary durable

All 14,600 inspected Dockerfiles start from Ubuntu 22.04. Of these, 5,135 have a separate `base_install.sh`: **5,134 are byte-identical**, and one is a variant. The shared script installs substantial native/scientific tooling, including CPU PyTorch. Build and convert these two resolved base environments once, then reference their immutable digests before copying task fixtures.

The other 9,465 Dockerfiles copy and execute a monolithic `post_install.sh`. Task-specific file generation changes that COPY input, invalidating the dependency installation inside the same script. Conservative inspection found recognizable initial dependency statements in 9,463 scripts, forming 2,883 text groups. The largest group covers 1,376 tasks; the top 25 cover 4,195; the top 100 cover 5,468.

Extract those initial provisioning stages into shared bases, starting with the highest fanout. These are candidate groups, not proven equivalent environments: pin the resolved packages and validate the transformed task state. Do not replace all tasks with an oversized package superset; extra tools, changed versions, or repaired deliberately broken files can change the benchmark.

### Terminal-Lego: share verifier dependencies independently of task files

The first FROM is Python 3.13 Bookworm, Ubuntu 22.04, or Node 20 slim in 12,413/13,825 Dockerfiles (89.8%). Applying the pinned verifier splitter to every task produced:

- 1,332 distinct bootstrap-plus-warm-command text plans; the most common covers 8,381 tasks (60.6%), and the top 25 cover 11,029 (79.8%).
- 447 distinct warm commands; one covers 11,615 tasks (84.0%).
- 13,825 distinct full verifier fingerprints, because those include task-specific script content.

The common warm command prepares Python 3.13, pytest 8.4.1, and pytest-json-ctrf 0.3.5. Today the index appends this dependency preparation **after the original task Dockerfile**, including its task-specific COPY/RUN operations. Consequently, shared command text often has a different parent filesystem and cannot reuse its BuildKit result or EROFS component.

Publish the interpreter and verifier dependency tree as a versioned, namespaced toolkit. Keep task-specific verifier identity in small task metadata. Share the dependency artifact by resolved packages, interpreter, architecture, and ABI—not the hash of the complete grading script. OS package changes remain part of a compatible base; an identical shell command on Fedora, Ubuntu, or Alpine does not establish compatibility.

Preserve the existing CA-isolation repair and other intentional task breakage. A toolkit must not silently restore system certificates or overwrite files the agent is supposed to repair. Grading code remains in the grading phase; only its permitted dependencies are precomputed.

### OpenSWE: Python bases help, but dependency stages matter more

36,392/36,884 recipes use 11 literal `openswe-python` versions, including stage aliases. Precompute those foundations, with qualification for legacy versions. However, 36,033 recipes have a RUN mentioning pip, conda, Poetry, or uv after a literal `COPY repo /testbed` (a textual count, not execution tracing). The current recipe generator substitutes a clone and exact checkout for that COPY, so changing the source revision still invalidates subsequent dependency work.

The next inventory needs dependency files at the exact Git revisions: requirements/constraints, lockfiles, build-system metadata, native package requirements, and toolchain versions. Group by their content and resolved environment, then install dependencies before the task checkout wherever semantics permit. Fetch repository objects once per repository/mirror rather than cloning the same history per task.

Do not move `pip install -e .` or source-dependent native compilation ahead of its required source. Separate reusable dependency wheels/toolchains from the project build, and retain source-sensitive inputs in the latter's key. The 10,501 distinct repositories make a single generic Python environment insufficient.

### Agent runtime: stop transferring the same archives per sandbox

At `7856cc1`, `offline.py` uploads, verifies, and extracts the offline Python bundle and agent harness into each new sandbox. Its host-side LRU avoids repeated local file reads, not network transfers or per-sandbox extraction.

These are strong toolkit candidates: immutable Python under `/opt/verifiers-offline/<digest>` and separately versioned agent tooling, with writable state private to each sandbox. Some current harness paths are shared root locations, so composition needs path/ABI validation and conflict handling. Internal `EnvironmentManifest` composition already exists, but public build/create wiring and readiness validation need implementation; this is not an existing SDK switch.

## What actual registry layers can share

The table applies the backend's current 64 MiB grouping algorithm to the inspected linux/amd64 manifests and ordered parent diff-ID chains. Component identities assume a fixed EROFS format. It measures potential reuse, not already-cached production components or measured EROFS byte savings.

| Selected images | OCI layer counts | Compressed input across three images | Reusable component identities across the three |
|---|---|---:|---:|
| Scale-SWE Monty | 1 each | 26.22 GB | 0 |
| Scale-SWE qontract-reconcile | 1 each | 1.73 GB | 0 |
| Scale-SWE fonttools | 1 each | 2.23 GB | 0 |
| R2E pandas | 14 each | 3.29 GB | 0 |
| R2E numpy | 16 each | 1.71 GB | 0 |
| Multi-SWE Checkstyle | 13 each | 1.64 GB | 3 |
| Multi-SWE Hugo | 16 each | 2.06 GB | 6 |
| SWE-Smith, three popular Python environments | 12 each | 5.77 GB | 5 |
| SWE-Lego SymPy | 15 each | 3.67 GB | 1 |

Checkstyle's duplicate OCI blobs account for about 56% of the three-image compressed total; Hugo's about 44%. R2E's samples share an OS layer, but it is grouped with differing subsequent layers, eliminating reuse of the planned EROFS groups. Explicit stable base cut points can recover that OS sharing, but do not turn the remaining large, distinct dependency layers into shared data.

For flattened Scale-SWE, do not expect different PR images to deduplicate merely because they are from the same repository. The bounded sample proves this problem exists; it does not establish the flattening rate of all 17,202 images. [registry-manifests.json](../benchmarks/image-precomputation-2026-09-30/registry-manifests.json) records all 27 exact references, resolved digests, layer sizes, and diff IDs.

## BuildKit and EROFS: retain both, remove repeated materialization

BuildKit caches build execution. EROFS artifacts are what workers mount. A warm build cache is not a launch-ready environment, and a ready EROFS environment does not require a worker to have BuildKit data.

The current expensive fallback can pass through:

```text
BuildKit content/snapshots → OCI registry → Docker Engine overlay2 → EROFS → registry → worker
```

BuildKit's docker-container store and Docker Engine's image store are separate. On a cold or unsupported selective-conversion path, the publisher pulls the just-built image into Docker Engine to prepare EROFS. A no-op external-image import also goes through a generated FROM build. This creates avoidable transfers and filesystem materialization.

The first remedy is to execute this path once offline and retain the output. Next, give external OCI imports a direct digest-pinned import/conversion path, and qualify a BuildKit-to-EROFS handoff for locally built snapshots or OCI exports. Keep whiteout, hardlink, ownership, xattr, and digest verification semantics. Raising the selective extraction limit without bounding scratch space is not a solution for 8.7 GB compressed layers.

Current EROFS group identity includes ordered uncompressed diff IDs, the parent chain, and format/mkfs settings. Equal-looking files under different OCI histories do not automatically share. Keep this correctness property. Precompute stable prefixes, add explicit base boundaries where needed, and version the grouping layout. Namespaced independent toolkits should use explicit composition, not masquerade as arbitrary reusable OCI upper layers. Respect the 33-component environment limit when reserving toolkit slots.

A longer-term option for flattened sources is common-file extraction or shared data blobs with separate per-image metadata. This needs an offline file/chunk inventory and mount/GC/signature design. EROFS documents deduplication and external data-device facilities, but the current backend does not expose cross-image file sharing through them. Merely enabling `-E dedupe` is not a shared store across separately generated images. See the [EROFS build documentation](https://erofs.docs.kernel.org/en/latest/mkfs.html) and [upstream mkfs manual](https://kernel.googlesource.com/pub/scm/linux/kernel/git/xiang/erofs-utils/+/88fc3faa57f711e3900db0692489552c4883212c/man/mkfs.erofs.1). Treat this as a measured prototype, not a prerequisite for protecting the next run.

## A predictable preparation and retention contract

Use one explicit progression: **planned → preparing → published → validated/ready**. Publication is atomic; a failed or partial result is never ready. A separate worker-warm status may improve latency without changing artifact validity.

1. **Canonical identity.** Bind platform, base manifest digest, recipe/schema version, actual context contents, Git/LFS revisions, build arguments, dependency/toolchain artifacts, and runtime/toolkit versions. Exclude checkout location and destination labels. At present `verifiers-ucloud` hashes recipe JSON containing absolute context paths; moving identical inputs can change the key. Conversely, paths alone do not verify mutable context bytes. Record resolved external dependencies; any deliberate dependency refresh creates a new preparation version.
2. **Durable artifact lookup.** Resolve a prepared environment independently of build-job history. The integration currently polls the old build ID and rebuilds on a 404 even when a published image may still exist. Verify the artifact and its complete component closure before resubmitting. `prepared_image` and source-only manifest mappings must also bind the exact recipe identity.
3. **One producer per artifact.** Use a durable cross-process claim with bounded lease, fencing, and a post-lock ready check. The importer currently permits interval-based resubmission, while component locks only coordinate processes sharing a local builder directory. These can duplicate expensive work across gateway processes or builders. Retries should attach to existing work; timeouts must not manufacture additional producers.
4. **Retention roots.** Pin every ready environment's manifests and transitive component blobs for the run or declared pool lifetime. BuildKit cache retention, builder VM lifetime, published artifact retention, and worker chunk-cache eviction are separate policies. Pruning must preserve referenced components even if no sandbox currently uses them.
5. **Run admission.** In strict training mode, validate and pin every image in the declared task pool before spending GPU time. Ordinary sandbox creation then resolves prepared artifacts without running apt, pip, Docker builds, or full image conversion. Missing preparation is reported before the run. For predictable task sequences, a rolling lookahead window is an optional optimization; it cannot guarantee readiness for arbitrary adaptive sampling. Never silently filter tasks to whatever happens to be cached.

The existing `ucloud_index_pools.py`, `ucloud_build_contexts.py`, and `prepared_image` fast path are useful starting points. Full-image precomputation can use the current SDK; general toolkit composition requires additional backend/integration support.

BuildKit cache selection also needs a family/dependency-stage reference, rather than only a whole-Dockerfile recipe key, whole-context affinity, or eight recent cache candidates. The production configuration budgets 512 entries/32 GiB/seven days for shared build cache. That is an opportunistic execution cache, not the durable catalog for this corpus. Current exports use `mode=min`; selectively export valuable intermediate dependency stages with `mode=max` where necessary instead of exporting every intermediate from every task.

Budget storage from unique blob unions. OCI final images and BuildKit caches can share identical compressed blobs in the registry; EROFS uses different bytes and requires its own budget. Include temporary conversion high-water space, concurrent work, and free-space headroom. Worker warming should fetch metadata and measured startup/test working-set chunks under an I/O budget, not download every complete image to every worker.

## Implementation order and acceptance

**First, prevent live cold-image surprises.** Implement the durable catalog/ready check and import deduplication, prepare the selected pool through existing builders, and pin its artifact closure. This directly addresses long import/build waits without waiting for a new filesystem design. The exact training pool is needed to prove complete coverage, but not to implement the mechanism or prepare the high-fanout foundations.

**Then factor and publish the high-fanout artifacts.** Start with the two TMax heavy bases, SWE-Smith's repository environments, Terminal-Lego's common verifier runtime, and the offline agent/Python bundles. Follow with TMax's most common initial dependency groups and OpenSWE's Python foundations plus dependency-file clusters. Rank work by expected avoided cold cost and task reuse per additional retained byte; until the sampler is known, corpus counts are only a prioritization proxy.

**Next eliminate duplicate conversion I/O.** Add direct external-image import and qualify a producer-to-EROFS handoff. In parallel, inventory common files in a bounded sample of the flattened Scale-SWE family and compare upstream rebuilds against shared-data storage. Do not launch a blind full-corpus import based on image counts alone.

Qualification must cover distinct, realistic images and exact task state. Compare original and factored filesystems, package versions, ownership, links, xattrs, working directories, Git commits, and both baseline/grading behavior; include deliberately broken CA and whiteout cases. Verify that moving a checkout does not change its canonical identity, that a dependency change does, and that a missing build record does not rebuild a healthy retained artifact.

For a declared-ready mixed pool, run 500–1,000 concurrent simulated agent sandboxes with create/exec/file/teardown traffic and representative startup/test commands. Record queue time, artifact lookup, mount, startup reads, toolkit setup, gateway TLS/network/registry I/O, and first-command p50/p95/p99 separately. Assert **zero cold builds/import conversions during the measured ready-pool phase**, no duplicate preparation under retries, and no loss of pinned artifacts across builder replacement and pruning. Include worker-cache misses: registry-ready does not imply that all workers are already warm. These are acceptance criteria, not results obtained by this static study.

## Full training/evaluation scope

The family list provides the scope of the preparation catalog, but does not specify dataset revisions, heldout row IDs, sampler weights, or per-run concurrency. Do not double the upstream image counts merely because a family appears in training and evaluation. Resolve both selections, deduplicate their immutable artifact digests, and retain separate selection manifests.

| Family | Training | Evaluation | Preparation target / evidence |
|---|---|---|---|
| BIRD SQL | Yes | Dev SQL | Deferred by user; exact implementation not verified |
| MultiSWE | Yes | — | Shared dependency prefixes and exact task environments; default pool analyzed |
| OpenSWE | Yes | — | Python foundations, dependency environments, exact task checkout; pinned recipes analyzed |
| R2E-Gym | Yes | Heldout | Immutable task images plus grader staging; exact heldout membership unknown |
| ScaleSWE | Yes | Heldout | Advance image conversion, especially flattened inputs; exact heldout membership unknown |
| SWE-Lego | Yes | Heldout | Task/dependency images; exact heldout membership unknown |
| SWE-rebench v2 | Yes | — | Resolved public images and preparation tools |
| SWE-smith | Yes | — | Repository environments and pinned instance refs |
| Terminal-Lego | Yes | — | Task image plus shared verifier toolkit |
| TMax | Yes | — | Shared dependency bases plus task fixtures |
| NeMo Calendar | Yes | Heldout | Deferred by user |
| NeMo Instruction | Yes | Heldout | Deferred by user |
| NeMo Pivot | Yes | Heldout | Deferred by user |
| NeMo Workplace | Yes | Heldout | Deferred by user |
| BrowseComp-Plus | — | BM25, GLM judge | Shared corpus/index/tokenizer and tool-server dependencies; code traced |
| OpenThoughts TBLite | — | Yes | Per-task `openthoughts-tblite/<slug>:latest` aliases must resolve to retained prepared images; pool not enumerated |
| Senior SWE-Bench | — | GLM 5.3 judge | All 50 pinned task images plus common verifier/validation dependencies |
| SWE-bench Multilingual | — | Yes | Public base images resolved from task Dockerfiles; pool not enumerated |
| SWE-bench Verified | — | Yes | Public instance images resolved from task Dockerfiles; pool not enumerated |
| Terminal-Bench 2 | — | Yes | Harbor environment images and any declared verifier environments; pool not enumerated |

Bash/OpenCode/Pi are agent-toolkit variants. The intended catalog should factor compatible harness binaries and dependencies separately from the task image, rather than make three independent copies of each task filesystem. Actual compatibility and the selected harness versions remain inputs to qualification. Agent and judge model names alone should not change the filesystem artifact key; they do change execution/evaluation configuration. A model change that also changes installed code or dependencies must change the corresponding toolkit key.

Train/dev/heldout views may share identical immutable dependency artifacts. Writable databases, sessions, agent edits, reward state, grader files, and task-specific answers must remain scoped to the correct episode/split. Reuse must preserve each taskset's existing grader staging; for example R2E removes hidden tests before the agent and restores them for grading.

### BrowseComp-Plus: prepare the retrieval service before evaluation

The pinned adapter builds or loads a host-side `bm25s` index in `ensure_cache()` during taskset loading. Its shared tool server then mmap-loads the index, loads the corpus and doc-ID mapping, and obtains the Qwen3-0.6B snippet tokenizer. The GLM judge is configured separately; this adapter does not define a distinct per-question sandbox image.

Prepare the corpus snapshot, document ordering, index, tokenizer assets, and service dependency environment once. Start and probe the shared service before admitting evaluation traffic. An index artifact must bind the corpus revision and doc order, BM25 implementation/version, k1/b, stopwords/stemmer, and relevant preprocessing code; snippet tokenizer identity belongs in the retrieval-service configuration. The current index-directory key includes only k1/b. A directory-exists check cannot establish that an index matches a freshly loaded mutable corpus revision.

The current concurrent cache builders can all do the expensive tokenization/indexing and discard losers after the rename. Claim this preparation work before computing it. Do not rebuild the index per evaluation worker or per agent variant. Replicate only where needed for placement/capacity, share immutable files locally, and load-test search latency and host memory alongside sandbox traffic.

### Senior SWE-Bench: remove installs from the grading path

Inspected all 50 task Dockerfiles, task configs, and `tests/test.sh` files at the adapter's exact upstream pin `e30b0e19fdbc4b099e752c6d5324f5b250aee3dc`. They span 12 repository names; 35 start from Ubuntu 22.04 and the rest use a mix of language/database bases. For this bounded evaluation suite, preparing all 50 exact task environments is a practical first target, subject to measured storage.

Every inspected `test.sh` contains the same eight-package verifier requirements block and installs it during grading. All 50 also contain a conditional mini-swe-agent installation for validation tasks. Separately, the pinned research adapter installs `fastapi` and `orjson` when upstream judges are enabled. Warm task images therefore do not remove these grading-time downloads, dependency resolution, or install failures.

Resolve and pin the shared verifier dependencies, including the adapter's additions, into compatible verifier toolkits. Prepare validation-agent tooling for the selected validation harness. Keep task-specific grader files staged at grading time, and keep tests, service initialization, and builds that depend on the agent's changes in the grading phase. Precomputation must not reuse a test result or compiled project from before the agent modified it. Package installation failures should be surfaced as infrastructure errors rather than allowed to look like a model failing the task.

GLM 5.3 is the requested judge configuration, not an additional copy of all 50 filesystem images. Verify the effective rubric, classifier, and validation-agent model overrides separately: the pinned adapter has distinct override variables and non-GLM defaults. A working image cache alone does not confirm that all stages use the intended judge.

### Readiness includes the complete episode

The readiness catalog needs to cover **agent setup, execution, and grading**, plus shared external services. Harbor supports a separately provisioned verifier sandbox, potentially using a different image. This behavior was traced in the available local verifiers checkout at `555a17a62ee36a2ffe96772b8191402e44f30c43`; that checkout is not asserted to be the user's deployed framework revision. Verify the actual task configs and runtime revision before sizing verifier overlap. Preserve requested grading isolation rather than disabling it to reduce sandbox count.

Extend qualification through finalization and grading: no unexpected dependency downloads, no late cold verifier image, healthy BM25 service, isolated mutable state, and correct judge configuration. Reserve capacity for agent/grader overlap and measure time waiting on judge inference separately from sandbox and registry time. Precomputation removes cold preparation costs; it cannot remove the work of running tests or obtaining judge responses.

Immediate scope is the nine SWE/terminal training families and six additional evaluation families. Start with the largest shared artifacts from the original plan, then fully prepare the bounded evaluation suites and their verifier dependencies. BIRD/NeMo definitions and exact heldout membership do not block these changes, but their deferred status must remain visible rather than being counted as ready.

## Source map

- Recipe generation and preparation reuse: [ucloud_index_pools.py](https://github.com/rlrs/research-environments/blob/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92/tools/ucloud_index_pools.py).
- Verifier dependency splitting: [terminal_lego/verifier.py](https://github.com/rlrs/research-environments/blob/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92/environments/terminal/terminal_lego/terminal_lego/verifier.py).
- Multi-SWE baseline preparation: [images/build.py](https://github.com/rlrs/research-environments/blob/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92/environments/swe/multiswe/images/build.py).
- Client-side preparation identity and build receipts: [image_builds.py](https://github.com/rlrs/verifiers-ucloud/blob/7856cc18faceae31aa533605ea812d81b92a85ea/src/verifiers_ucloud/image_builds.py).
- Repeated runtime archive transfer: [offline.py](https://github.com/rlrs/verifiers-ucloud/blob/7856cc18faceae31aa533605ea812d81b92a85ea/src/verifiers_ucloud/offline.py).
- Backend cache exports: [images.py](../../ucloud_sandboxes/images.py) and [build_cache.py](../../ucloud_sandboxes/build_cache.py).
- Runtime component grouping and conversion: [environment_builder.py](../../ucloud_sandboxes/environment_builder.py), [environment_artifact.py](../../ucloud_sandboxes/environment_artifact.py), and [environment_manifest.py](../../ucloud_sandboxes/environment_manifest.py).
- Import scheduling: [image_import.py](../../ucloud_sandboxes/image_import.py).
- BrowseComp shared index: [corpus.py](https://github.com/rlrs/research-environments/blob/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92/environments/search/browsecomp_plus/browsecomp_plus/corpus.py) and [search.py](https://github.com/rlrs/research-environments/blob/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92/environments/search/browsecomp_plus/browsecomp_plus/servers/search.py).
- Senior SWE adapter and judge wiring: [taskset.py](https://github.com/rlrs/research-environments/blob/c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92/environments/swe/senior_swe_bench/senior_swe_bench/taskset.py); [pinned upstream tasks](https://github.com/snorkel-ai/senior-swe-bench-v2026.06/tree/e30b0e19fdbc4b099e752c6d5324f5b250aee3dc/tasks).
