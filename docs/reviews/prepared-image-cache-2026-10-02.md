# Prepared image cache: review and recommendation (2026-10-02)

The question: is the prepared cache a good design, and does it fit the RL-scale plan
(`docs/rl-scale-architecture-plan.md`)? The cache is the offline machinery that
avoids fully building every training image: foundations, prepared sources,
dependency prefixes, flat and layered deltas, and recipe rewriting.

Inputs:
- the training selection `all-cached-training-tasks-with-terminal-lego-2026-10-01.zip`;
- a read-only map of the code, docs and benchmarks at `5492b89`;
- the 0.8.0/0.8.1 production canaries.

## Verdict

**Short answer.** Keep the worker side. Replace the strategy of "prepare as much as
possible, build the rest live".
- The **worker side** is right and matches the plan: signed EROFS components,
  lazily fetched 256 KiB chunks, OverlayFS composition at attach, and the
  metadata-hint and trace prefetch.
- The **preparation side** saves build work in the wrong place. It moves most of the
  remaining build work onto the rollout's start path, where it is slowest, least
  reliable and least reproducible.
- **The reuse unit is the OCI layer chain.** Sharing needs identical layer prefixes,
  and most of the machinery exists only to manufacture them.

## What the training selection shows

From the 2026-10-01 selection: 130,253 task rows, 66,786 image selectors.

| Families | Images | Rows | State at request time |
| --- | ---: | ---: | --- |
| SWE-smith, ScaleSWE, MultiSWE, R2E-Gym, SWE-Lego, SWE-rebench v2 | 2,860 | 66,327 | Complete: attach only (SWE-smith is 118 images for 63,585 rows) |
| OpenSWE | 35,549 | 35,549 | Foundation only. Live: checkout, package manager, network; compilation for 10,992 |
| TMax | 14,600 | 14,600 | Foundation or source. Live: context transfer and task setup |
| Terminal-Lego | 13,777 | 13,777 | Foundation or source. Live: verifier and task setup |

- **Live builds:** 63,926 images (49% of rows) need a builder build the first time a
  rollout uses them.
- **Distinctness:** OpenSWE's 35,549 images come from 10,326 projects; pandas alone has
  550 tasks, each at its own commit.
- **Build cost:** request-time builds on the campaign ran p50 61–77 s, p95 142–167 s.
  A fresh builder adds about 4 minutes, and the 2026-09-30 incident logged 6,176
  build 503s.
- **A uniform 500-rollout burst** would put about 245 sandboxes behind live builds on a
  handful of builder slots. Estimate: the tail waits tens of minutes, and that
  dominates everything W9 measures.

## Findings

**1. Live builds are on the critical path, and they are not reproducible.**
- The remaining task steps (apt, pip, git and compilation) run with live network
  access when the first rollout needs the image.
- Upstream packages are unpinned (`docs/image-foundations.md`), so two builds of the
  same task can differ. An evicted image is rebuilt (`image_evicted`,
  `rebuild_required`) and may then differ from the one earlier rollouts saw.
- For RL that is a correctness problem: the environment is part of the reward.
- It is also a reliability problem: one production canary hit a GitHub 504.

**2. Sharing depends on layer identity, not content.**
- An EROFS component's key is (layout, parent ChainID, ordered diff IDs)
  (`environment_artifact.py:198-207`).
- Two ScaleSWE images with 98.55% identical file bytes shared zero components.
- Foundations, FROM rewriting, flat deltas, anchors and layered exports exist to
  manufacture identical layer prefixes offline. That is about 5.4k lines of scripts
  and about 1k lines of package code, written in three days. They keep roughly 30
  state stores, 6 retention-owner kinds and 5 image-ID namespaces.
- Their equivalence is checked on samples only, and 155 rewritten OpenSWE recipes are
  "unknown" to the audit.

**3. Storage is paid twice and is running out.**
- Every prepared image is kept as OCI (needed as a build input) and as EROFS, about
  2.6× its compressed size.
- The registry went from 3,000 to 4,000 provider units in about a day.
- The capacity projection fits only about 855 more source images.
- Changing the EROFS layout (layout 2, which fixes `.pyc` invalidation) means
  republishing the whole corpus.

**4. It has no fit with the plan's runtime composition.**
- The plan's D5 diagnosis names this machinery. Its fix, C2.5 toolkits and C3.1
  commit, removes harness rebuilds but keeps offline factoring for datasets.
- No plan item yet removes live task builds from the start path.

## Recommendation

Precompute every training image once, and make sharing content-addressed so that
precomputing everything is affordable.

**R1. Chunk-level content addressing (new C2.13).**
- Publish components with chunk-aligned file data in a content-addressed chunk
  store, instead of one blob per layer group. Identical file chunks then dedupe
  across images regardless of their layer history.
- EROFS supports this directly: chunk-based files over external blob devices,
  `mkfs.erofs --chunksize` / `--blobdev`. This is the layout Nydus RAFS v6 uses.
  The exact flags and the dedupe ratio are to be verified in the spike.
- Images become small metadata plus references to shared chunks.
- Effects:
  - the worker chunk cache is already keyed by chunk digest, so a chunk is fetched
    once per node across images;
  - it is the natural format for the C2.6 store tier;
  - foundations, anchors and flat deltas stop being needed for storage. They can
    stay as build accelerators, through the BuildKit cache, where they help.
- **Spike first.** Republish a sample (200 OpenSWE, 100 ScaleSWE, 100 TMax) both
  ways. Compare stored bytes, chunk-cache bytes per burst and attach latency. This
  decides whether R2 fits the registry.

**R2. Precompute the whole training split (new C2.14).**
- Build all 66,786 images once, before training, and freeze them.
- Training images get a protected retention owner and are never evicted or rebuilt
  implicitly.
- Rough cost, at the measured p50 of about 70 s: about 1,200–1,300 build-slot hours.
  That is 1.5–3 days of wall clock at 16–32 concurrent builds. Builder hours are
  cheap next to a day of RL training that waits on builds.
- Turn on the layout-2 writer (erofs-utils 1.9+ on builders) first, so the corpus is
  built once, with `.pyc` mtimes preserved and metadata hints attached.
- **Option: build through sandboxes instead of BuildKit.** Run each task's remaining
  steps inside a sandbox started from its foundation, then publish the upper as a
  component with C3.1 commit. A CCX63 runs hundreds of sandboxes but only a few
  BuildKit builds, so this parallelizes far better. The cost is translating
  Dockerfile semantics (COPY context, ENV, USER, WORKDIR, ARG) and qualifying RUN
  under gVisor. Pilot it on TMax and Terminal-Lego, whose remaining steps are
  scripts.

**R3. Keep a build-ahead path for new tasks (part of C2.7).**
- Tasks added after the precompute go through the hydration API: the dataloader
  names upcoming tasks, and their builds run ahead of the trainer.
- A live build at create time becomes the exception and is reported as such.

**R4. Retire machinery as R1 and R2 land (with C2.10).**
- The flat/layered delta pipeline, anchors, import-alias writes into
  `images.sqlite`, the offline recipe-index rewrite, and most per-work-dir state can
  go once every training image is a frozen, content-addressed artifact.
- What remains is one register API (C2.10) and the BuildKit cache.

## Consequences for W9

- **Fair starting point.** The benchmark's starting point should be the precomputed,
  layout-2, content-addressed corpus. Measuring today's state would mostly measure
  builder queueing.
- **Reference run.** One C9.2 run on today's corpus is still worth keeping as the
  "before" for the post: same scenario, same image sample.
- **Order:**
  1. C9.1 registry read limit;
  2. R1 spike;
  3. layout-2 writer;
  4. R2 precompute;
  5. C9.2 runs;
  6. C9.3 cache seeding.
- C9.1 and the R1 spike can start now. R2 needs the R1 decision. C9.2 code can be
  written against the selection list in parallel.

## Not established

- **OpenSWE delta sizes are unmeasured.** The checkout plus installed packages per
  task may be hundreds of MB. Dedupe across commits of one project is what R1 has
  to show.
- **R2's build cost** extrapolates the campaign's p50. Compilation-heavy OpenSWE
  tasks and failures will raise it, and some recipes will fail and need fixes.
- **The sandbox-build option** depends on how many recipes run correctly under
  gVisor and translate cleanly from Dockerfile form.

## Addendum: decisions (same day)

- **Use Nydus, not our own chunk store.** RAFS v6 is EROFS, so it mounts with the
  kernel driver over our NBD path. Its chunk dictionary gives cross-image dedupe,
  and `nydusify` converts our existing OCI images. Our signed roots, chunk cache and
  prefetch stay. The chunk dictionary dedupes only against the chunks it lists
  (Nydus's own tests: 10–55% on image versions), so ours must cover every image
  already converted.
- **Precompute, after all.** A training run touches 500 tasks × 100–1,000 steps.
  Long runs therefore use nearly every image, and building ahead of the sampler
  would need about 70 concurrent builds. The corpus is built once, with lockfiles,
  and frozen. Build-ahead covers only tasks added later.
- **Constraints:**
  - reuse the existing prepared images as the conversion input;
  - no new Docker Hub pulls (C2.15 pull-through mirror);
  - the registry must not grow by many TB: converted images release their EROFS
    copies, and task images release their OCI copies.
- **Plan items:** C2.13–C2.15 in `docs/rl-scale-architecture-plan.md`.
