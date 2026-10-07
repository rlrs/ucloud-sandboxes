# Building training images on demand: a pilot (2026-10-07)

The question: C2.14 plans to build all 64k remaining training images ahead of training and
keep them. OpenSWE alone would take 3–10 TB. Are the builds cheap enough to run on demand
instead, shortly before the sampler needs each task?

The pilot built 250 foundation-backed training tasks through the production gateway (0.9.52
and 0.9.53, builders `cpu-amd-zen5-16-vcpu`). Each build used the task's exact training recipe,
checked against the selection's `recipe_sha256`, submitted as an integration would
(`Image.from_dockerfile`). The gateway substituted the prepared foundation as usual.

## Result

| | TMax | Terminal-Lego | OpenSWE |
|---|---|---|---|
| Tasks (seeded sample of foundation-backed training tasks) | 100 | 100 | 50 |
| **Built** | **100** | **98** | **40** |
| Build plus conversion, p50 / p90 | 28 / 191 s | 35 / 57 s | 97 / 238 s |
| of which `docker build` and push, p50 | 10 s | 20 s | 55 s |
| of which conversion to EROFS, p50 | 17 s | 14 s | 43 s |
| New compressed OCI bytes over the base, p50 / mean | **0.5 / 38 MB** | **122 / 132 MB** | **207 / 609 MB** |
| Foundations in the sample | 36 | 76 | 9 |
| First use of a foundation: regeneration, p50 / max | 91 / 421 s | 50 / 322 s | 110 / 363 s |

- **Throughput:** with 24 builds in flight, 5 builders ran up to 6 builds each, at 557
  builds/hour, with zero queue wait. The in-flight limit was the bound, not the builders. Where
  builders saturate was not measured.
- **Failures are recipe rot, the same live or ahead of time.**
  - OpenSWE, 10 of 50:
    - unpinned or yanked dependencies (`ray==0.7`, `django==5.0.9` "from versions: none");
    - `conda activate` without `conda init` in 4 recipes;
    - wheels that no longer build (`pygame`, `pillow 10.3.0`).
  - Terminal-Lego, 2 of 100: the verifier's apt step asks for `php-cli` and `php-curl`, which
    Debian bookworm no longer offers as candidates.
  - TMax: none.
- **Storage is family-dependent.**
  - **TMax:** most tasks add almost nothing (p50 0.5 MB). Tasks on the largest foundation
    (`foundation-tmax-e92c…`, 45 of 100) add about 55 MB each.
  - **Terminal-Lego:** about 120 MB per task, nearly all of it the same verifier bootstrap
    (uv, Python 3.13 and pytest) installed into every image after the task's own files. Those
    are identical files, so the chunk store should deduplicate most of it, but this pilot
    measured OCI bytes, not chunks. The harness toolkit could carry the verifier's Python
    instead.
  - **OpenSWE:** p50 207 MB, mean 609 MB, matching
    [openswe-deltas-2026-10-02](../openswe-deltas-2026-10-02/) (275 MB of new chunks per
    task). 35,549 tasks are several TB: too much to keep them all.
- **A foundation's first build costs a regeneration.**
  - Foundations were released to the chunk store, so the first build on each one regenerates
    its OCI copy: 1–7 minutes, two at a time per gateway.
  - The regenerated copy is kept 30 days after last use.
  - The first build on a foundation also re-converts the regenerated base: 350 MB p50 for TMax,
    1.3 GB for OpenSWE. Later builds convert only their own layers.

## Chunk-store bytes (TMax and Terminal-Lego)

`scripts/measure_chunks.py` cut every file in each built image's added layers into 256 KiB
chunks (sha256, zstd level 3) and charged an image only for chunks no earlier image of its family
had added. Base chunks were not loaded, so this is an upper bound.

| | Compressed per task, p50 / mean | **New chunks, p50 / mean** | All tasks, no dedup | **All tasks, with dedup** |
|---|---|---|---|---|
| TMax (100) | 0.5 / 37 MB | **0.01 / 8.5 MB** (later half: 4.8) | 540 GB | **about 70 GB** |
| Terminal-Lego (98) | 118 / 127 MB | **2.3 / 11.4 MB** (later half: 15) | 1.75 TB | **about 210 GB** |

- The 100 TMax tasks added 0.85 GB of new chunks, the 98 Terminal-Lego tasks 1.1 GB.
- Terminal-Lego's repeated verifier install deduplicates, as expected.
- Prebuilding both families costs about 300 GB in the chunk store, but only if the built images
  are converted into the chunk store and their per-image EROFS and OCI copies released.
  Otherwise the registry keeps about 80–220 MB per image.

## Found and fixed on the way

- **0.9.52: building from any released foundation had been broken since the move to UCloud.**
  - Stored chunk locators keep the store node URL of the day they were built, and production's
    still named the Hetzner store node. Base regeneration refused them, so every build from a
    released foundation failed: all of OpenSWE, TMax and foundation-backed Terminal-Lego.
  - Workers were unaffected: `nydusd` names objects by key.
  - The Python reader now takes only the object path from a locator.
- **0.9.53: one transient store error failed a whole foundation for 10 minutes.**
  - The rebuilt store node's cache was cold. One S3 fill missed its deadline, and the store
    node answered 503.
  - Regeneration took the 503 as final. With the 600 s failure backoff, that failed all 45 builds
    on TMax's largest foundation.
  - Regeneration now retries 502/503/504 for about a minute.
- **The pilot's own inputs:** the local Terminal-Lego and TMax checkouts are partial (a
  `blob:none` clone and a sparse checkout). 41 Terminal-Lego contexts lacked task files and 45
  TMax contexts lacked `_fixtures/`. `scripts/prepare.py` now writes every context file from
  its git object. Training's own loader must ship the whole `environment/` tree too.

## What it means for C2.14

On-demand builds work and are fast enough to hide behind a one-step sampler lookahead. A
fresh task needs 0.5–4 minutes before its first rollout, plus a regeneration on the first use of
a foundation. Suggested split:

- **OpenSWE:** build on demand, shortly before the sampler needs each task. Cache the built
  images with eviction, and exclude tasks whose recipes fail (20% here) from the selection.
  Storing all of it is the multi-TB case.
- **TMax:** prebuild: about 70 GB of chunks for all 14,600 tasks. None failed.
- **Terminal-Lego:** prebuild too: about 210 GB of chunks for all 13,777 tasks. 2% fail.
- **Before training starts on a new selection:** regenerate its foundations ahead (one per
  foundation, minutes each), not during the first step.

Not measured: OpenSWE's deduplicated bytes, builder saturation, and whether
the built images pass their tasks' verifiers.

## Files

- `scripts/prepare.py`: sample and lay out contexts. `scripts/run_pilot.py`: build through
  the gateway. `scripts/summarize.py`: the table above.
- `raw/tasks.json`, `raw/results.jsonl`: the 250 tasks and their final builds. 45 TMax builds
  come from the retry on 0.9.53 with complete contexts.
- `raw/first-run-results.jsonl`: the first run, with the store-related and context-related
  failures.
- `raw/summary.txt`: `summarize.py`'s output. `raw/chunks.json`: `measure_chunks.py`'s per-image bytes.
