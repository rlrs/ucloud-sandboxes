# What OpenSWE task images add over their foundations, and how far slimming cuts it (2026-10-02)

The question: spike S10 ([`../nydus-spike-2026-10-02/`](../nydus-spike-2026-10-02/)) measured
100–276 MB of new 256 KiB chunks per OpenSWE task. At that rate, precomputing all 35,549 OpenSWE
tasks needs 7–9 TB, more than the "not many TB" budget in
[`rl-scale-architecture-plan.md`](../../rl-scale-architecture-plan.md) C2.14 allows. What do the
task deltas hold, and which build-recipe changes shrink them without breaking the task?

## Result

- **Today's deltas: 275 MB of new chunks per task** (mean; median 216, p90 569; zstd, 29 tasks).
  That reproduces S10: 276 MB for later pandas commits. All 35,549 tasks come to **about 9.7 TB**.
- **Where the bytes go** (share of new chunk bytes):
  - installed packages, 34%;
  - package caches (pip, conda tarballs), 28%;
  - `.git` packs, 22%;
  - apt-installed system files, 8%;
  - build artifacts, 6%;
  - the source tree itself, 2%.
- **Recommended recipe slimming, variant (c), cuts that to 134 MB per task** (median 78, p90 346):
  - delete caches;
  - cut the git history to the task commit, keeping the commit's real SHA;
  - make `.pyc` files hash-based;
  - strip debug info from extensions built in the tree.

  All of OpenSWE then comes to **about 4.7 TB** (3.8–4.8 TB depending on how later commits are
  modelled, 2.8 TB at medians). Expect less as the corpus grows, see
  [Extrapolation](#extrapolation-to-35549-tasks).
- **No harness-visible change.** Across 29 tasks × 8 variants these all matched today's images:
  - `git status`;
  - test collection;
  - the OpenSWE eval script, run both unpatched and gold-patched.

  The only visible differences are `git log`, and versioneer version strings that lose their
  `+N.gSHA` suffix.
- **Don't run `git gc`** (it rewrites the pack per task: 356 MB per task). **Don't use a
  placeholder commit:** it saves nothing over the shallow cut, drops the base SHA that 870 eval
  scripts name, and hides build-time edits to tracked files.
- **(c) is not enough for "not many TB".** About 65% of what remains is installed packages:
  dependency versions that differ from task to task. The next lever is the C2.14 lockfiles: prefer
  versions already in the chunk store. Without that, OpenSWE needs its own budget of about 3–5 TB,
  or precompute only the training split.

## Setup

- **VM:** one `sandboxes-spike-openswe` server: cpx62 (16 vCPU, 30 GiB), snapshot `438728121`,
  10.42.0.43. VM init was not run.
  - **Runtime:** created 14:55:40Z, deleted 16:10:25Z, so **1 h 15 min**. The Hetzner API
    confirmed that no server of that name remained.
  - **Disk:** Docker data lives on the snapshot's XFS loop image. I grew it from 64 to 450 GB on
    the VM, a VM-local change only.
- **Builds:** Docker 29.8.1 with the legacy builder, as in S10, using `--network host`.
  - Every `FROM` is the digest-pinned registry foundation from
    `build/openswe-foundations-expanded-20260930/coverage.sqlite`.
  - Builds fetched from GitHub, PyPI, conda and Debian mirrors. There were no Docker Hub pulls,
    and nothing was pushed.
- **Production access was read-only.** The VM pulled the 11 OpenSWE foundations from
  10.42.0.2:5000. Nothing was written to the registry, `images.sqlite`, config or services. The
  gateway was used only as a jump host plus a staging file under `/root`, removed afterwards.
- **Eval definitions:** taken from the gated `GAIR/OpenSWE` dataset, `openswe_oss.jsonl` (36,884
  rows, streamed once), as used by verifiers' `OpenSWETaskSet`. Each row carries:
  - `eval_script`, which applies the test patch, runs pytest and prints `OPENSWE_EXIT_CODE`;
  - `test_patch` and the gold `patch`.

  `FAIL_TO_PASS` and `PASS_TO_PASS` are empty in every selected row, and `install_config.test_cmd`
  is empty. The reward is `OPENSWE_EXIT_CODE == 0`. Eval scripts and gold patches are task
  solutions, so they are not stored here; only aggregate scan counts are
  ([`raw/eval-script-git-usage.json`](raw/eval-script-git-usage.json)).

### Tasks

[`raw/tasks.json`](raw/tasks.json) lists the 35 tasks attempted with their recipes, and
[`raw/order.json`](raw/order.json) gives the processing order.

- **29 tasks built:**
  - 13 pandas-dev/pandas commits: the 9 S10 successes plus 39341, 42222, 46301 and 50682;
  - 16 tasks from 14 other projects:
    - the 9 S10 successes;
    - compile-heavy scikit-learn-30152 (meson) and astropy-16038 (C extensions);
    - getmoto/moto-7212;
    - one later commit each of moto (7752) and scikit-learn (29021);
    - conan-15665 and xarray-9194.
- **5 pandas recipes failed against today's upstream**, all in dependency resolution:
  - 47327: `pip install --upgrade pip` under the conda env;
  - 58452 and 60697: the `meson==1.2.1` / `meson-python==0.13.1` pins;
  - 52264: `numpy<1.25` built from source;
  - 32121: `conda install numpy==1.19.5` is unsatisfiable today.

  Across S10 and this run, 27 distinct pandas recipes were tried: 13 build today, and 14 failed
  (9 in S10, not retried here, and these 5). Every non-pandas recipe built, including the four
  whose clones failed under load in S10.
- **4 tasks built but fail their own eval at baseline, with or without the gold patch**, so this is
  upstream drift, not slimming:
  - pandas-63232 and 62428 segfault in pytest collection;
  - pandas-21799 and xarray-9194 fail tests.

## Method

**Tree accounting** ([`scripts/walk.py`](scripts/walk.py),
[`scripts/analyze.py`](scripts/analyze.py)):

- **Final trees, not layers.** Each image's final tree is streamed with `docker export`. This is
  what a RAFS v6 image built from the final tree, plus our chunk store, holds: files deleted or
  shadowed in later layers cost nothing.
- **Chunking.** Each regular file is cut into fixed 256 KiB chunks. A chunk is identified by the
  sha256 of its uncompressed bytes, and stored at its zstd size (level 3, per chunk).
- **Checked against Nydus.** On two full images the walker's chunk sets equal
  `nydus-image v2.4.5 create -t dir-rafs --chunk-size 0x40000 --compressor zstd` exactly, and
  compressed sizes agree within 0.15% ([`raw/calibration.json`](raw/calibration.json)).
- **Delta.** A task's delta is every path whose type, size, mtime, mode or link differs from its
  foundation's tree.
- **New chunk bytes** are counted per variant, in a fixed order (S10's order, then the added
  tasks). They are deduplicated against the chunk set of all 11 OpenSWE foundations plus every
  task processed earlier in that variant. That is S10's chunk-store method, except S10 also seeded
  the set with its 181-image sample.
  - Within one image, a chunk held by several files is charged to the most essential class:
    source > git > installed > build > other > cache > tmp.
  - Hard links count once, at their target.
- **"First" and "later":** "first" is the first task of a project in that order, "later" every
  further commit of the same project. All figures are in MB of 10^6 bytes; "zstd" is the stored
  size.

**Path classes** (`classify` in [`analyze.py`](scripts/analyze.py)):

| Class | Paths |
|---|---|
| source | files tracked by `git ls-files` in `/testbed` |
| git | `/testbed/.git`: `objects/pack`, loose objects, other |
| installed | `site-packages`/`dist-packages` (with `pyc` and `.so` split out), the rest of `/opt/conda` envs, extracted `/opt/conda/pkgs/<pkg>/` |
| build | untracked files in `/testbed`: `build/`, `*.o`, compiled `*.so`, generated `.c`/`.cpp`, `*.egg-info`, `.eggs`, `__pycache__` |
| cache | `~/.cache` (pip, uv, other), conda tarballs and index cache, apt lists and archives |
| tmp | `/tmp`, `/var/tmp`, logs |
| other | apt-installed system files (`/usr`, `/lib`, …), `/etc`, dpkg, home directories |

**Slimming variants.** [`scripts/slim.sh`](scripts/slim.sh) runs as the final build step, in a
container from the built image, which is then exported and committed:

| Variant | What it does |
|---|---|
| **(a)** | Removes `~/.cache`, runs `conda clean -a` (tarballs, index cache, unused extracted packages), and removes apt lists, `/tmp` and `/var/tmp`. Truncates logs. `/testbed` is untouched |
| (a)+gc | (a) + `git gc --prune=now`, keeping the full history |
| **(a)+shallow** | (a) + history cut at HEAD (`.git/shallow`), all refs and reflogs dropped, nearest tag re-created on HEAD so `git describe` resolves, repacked. HEAD keeps its real SHA |
| (a)+shallow, loose | as shallow, with objects unpacked to one zlib file each (identical objects dedupe across commits), `gc.auto=0` |
| (a)+placeholder | (a) + `.git` re-initialised with one fixed-date commit of exactly the tracked files |
| (a)+shared pack | (a) + objects replaced by one byte-identical pack per project (a bare clone made once on the VM), full history kept. Measured for pandas, moto and scikit-learn only |
| **(c)** | (a)+shallow, loose, plus three steps. Delta `.pyc` files are recompiled as checked-hash pycs (deterministic, and validated against the source on every import); Python 3.6 pycs are deleted instead. In-tree `*.so` get `strip -p --strip-debug` (mtime kept). setuptools `build/temp.*/*.o` are deleted, but never inside a meson/ninja build directory |

**Verification.** Every variant image ran [`scripts/verify.sh`](scripts/verify.sh) and
[`scripts/run_eval.sh`](scripts/run_eval.sh) with no network:

- **Git and imports:** `git status --porcelain` (exit code, line count, content hash); HEAD
  against the dataset's `base_commit`; `git fsck --connectivity-only`; the package's
  `__version__`.
- **Tests:** `git apply --check` of the test patch, then
  `pytest --collect-only -q <the eval script's test targets>`.
- **Full eval:** base, (a)+shallow loose, placeholder and (c) also ran the dataset eval script the
  way the harness does, with network and 4 CPUs:
  - unpatched (what an agent that changed nothing would score);
  - after the harness's gold-patch step (`git apply --whitespace=fix`, falling back to
    `patch --fuzz=5`), which is `validate_instance`.

## Per-task breakdown (today's recipes)

New chunk MB (zstd) by class; "(c)" is the same task after slimming variant (c).

| # | Task | Kind | Delta MB | New chunks MB (unc.) | **New chunks MB (zstd)** | source | git | installed | build | other | cache | tmp | (c) zstd MB |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | pandas-dev__pandas-20422 | first | 1,046 | 1,042 | **623** | 8 | 438 | 56 | 66 | 0 | 55 | 0 | 76 |
| 2 | pandas-dev__pandas-63232 | later | 1,258 | 1,217 | **723** | 13 | 411 | 108 | 21 | 72 | 97 | 0 | 229 |
| 3 | pandas-dev__pandas-22261 | later | 1,224 | 585 | **216** | 4 | 0 | 38 | 72 | 20 | 81 | 0 | 70 |
| 4 | pandas-dev__pandas-62428 | later | 2,469 | 1,196 | **301** | 6 | 0 | 43 | 19 | 1 | 232 | 0 | 83 |
| 5 | duck-dynasty__duckbot-1203 | first | 1,461 | 1,155 | **555** | 6 | 8 | 175 | 0 | 210 | 155 | 0 | 398 |
| 6 | OasisLMF__OasisLMF-1406 | first | 1,772 | 1,639 | **818** | 64 | 146 | 313 | 0 | 20 | 274 | 0 | 468 |
| 7 | androguard__androguard-411 | first | 902 | 534 | **310** | 10 | 117 | 30 | 0 | 72 | 82 | 0 | 112 |
| 8 | mps-gmbh__hl7-parser-32 | first | 18 | 7 | **3** | 0 | 0 | 3 | 0 | 0 | 0 | 0 | 0 |
| 9 | napari__napari-tiff-31 | first | 541 | 222 | **103** | 0 | 0 | 55 | 0 | 3 | 44 | 0 | 51 |
| 10 | newam__idasen-340 | first | 339 | 305 | **161** | 0 | 1 | 52 | 0 | 7 | 100 | 0 | 59 |
| 11 | aiidateam__reentry-50 | first | 70 | 37 | **17** | 0 | 0 | 12 | 0 | 0 | 5 | 0 | 7 |
| 12 | fake-useragent__fake-useragent-216 | first | 61 | 46 | **28** | 0 | 2 | 14 | 0 | 0 | 12 | 0 | 14 |
| 13 | rustedpy__result-58 | first | 36 | 25 | **11** | 0 | 0 | 7 | 0 | 0 | 4 | 0 | 7 |
| 14 | pandas-dev__pandas-54945 | later | 1,538 | 551 | **162** | 7 | 0 | 109 | 29 | 0 | 17 | 0 | 151 |
| 15 | pandas-dev__pandas-43447 | later | 2,042 | 1,285 | **502** | 6 | 0 | 302 | 16 | 0 | 179 | 0 | 333 |
| 16 | pandas-dev__pandas-45642 | later | 937 | 293 | **104** | 5 | 0 | 37 | 22 | 0 | 40 | 0 | 62 |
| 17 | pandas-dev__pandas-34736 | later | 916 | 317 | **102** | 5 | 11 | 26 | 22 | 16 | 22 | 0 | 67 |
| 18 | pandas-dev__pandas-21799 | later | 1,294 | 552 | **214** | 1 | 0 | 71 | 71 | 0 | 70 | 0 | 78 |
| 19 | pandas-dev__pandas-50682 | later | 1,135 | 509 | **207** | 6 | 0 | 87 | 18 | 7 | 89 | 0 | 121 |
| 20 | pandas-dev__pandas-39341 | later | 2,119 | 1,005 | **285** | 5 | 0 | 48 | 21 | 1 | 210 | 0 | 79 |
| 21 | scikit-learn__scikit-learn-30152 | first | 1,707 | 1,179 | **492** | 7 | 199 | 231 | 32 | 24 | 0 | 0 | 293 |
| 22 | astropy__astropy-16038 | first | 674 | 545 | **312** | 8 | 199 | 18 | 2 | 62 | 23 | 0 | 90 |
| 23 | getmoto__moto-7212 | first | 677 | 421 | **236** | 5 | 111 | 64 | 0 | 0 | 55 | 0 | 73 |
| 24 | getmoto__moto-7752 | later | 909 | 504 | **196** | 4 | 0 | 54 | 0 | 102 | 35 | 0 | 153 |
| 25 | scikit-learn__scikit-learn-29021 | later | 1,743 | 461 | **272** | 5 | 0 | 57 | 21 | 0 | 189 | 0 | 35 |
| 26 | conan-io__conan-15665 | first | 164 | 98 | **62** | 1 | 39 | 4 | 0 | 17 | 1 | 0 | 22 |
| 27 | pydata__xarray-9194 | first | 1,696 | 1,344 | **456** | 4 | 59 | 393 | 0 | 0 | 0 | 0 | 419 |
| 28 | pandas-dev__pandas-42222 | later | 943 | 140 | **34** | 1 | 0 | 17 | 16 | 0 | 0 | 0 | 26 |
| 29 | pandas-dev__pandas-46301 | later | 2,124 | 1,165 | **464** | 4 | 0 | 287 | 10 | 0 | 163 | 0 | 302 |

**Aggregated over the 29 tasks:**

| Class | Delta MB mean | median | p90 | New chunks (unc.) MB mean | **New chunks (zstd) MB mean** | median | p90 | share of new zstd |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| source | 35 | 36 | 53 | 22 | **6** | 5 | 8 | 2% |
| git | 240 | 202 | 441 | 61 | **60** | 0 | 199 | 22% |
| installed | 430 | 316 | 1,070 | 287 | **94** | 54 | 290 | 34% |
| build | 86 | 101 | 188 | 80 | **16** | 10 | 39 | 6% |
| other | 122 | 73 | 262 | 63 | **22** | 1 | 72 | 8% |
| cache | 184 | 99 | 380 | 121 | **77** | 55 | 193 | 28% |
| tmp | 0 | 0 | 0 | 0 | **0** | 0 | 0 | 0% |
| **total** | 1,097 | 1,046 | 2,057 | 634 | **275** | 216 | 569 | 100% |

**By subclass**, as mean MB per task:

| Subclass | Delta MB/task (base) | New zstd MB/task: base | (a) | (a)+shallow | (c) |
|---|---:|---:|---:|---:|---:|
| git/pack | 239 | 60.0 | 60.0 | 9.0 | 0.0 |
| cache/pip | 68 | 47.8 | 0.0 | 0.0 | 0.0 |
| installed/so (extension modules in site-packages) | 165 | 32.0 | 32.0 | 32.0 | 32.0 |
| cache/conda-tarballs | 112 | 26.5 | 0.0 | 0.0 | 0.0 |
| installed/conda-env-other (conda libraries, binaries, headers) | 94 | 26.1 | 26.1 | 26.1 | 26.1 |
| other/system-usr (apt installs) | 119 | 20.6 | 20.6 | 20.6 | 20.6 |
| installed/pyc | 52 | 20.6 | 20.6 | 20.6 | 13.6 |
| installed/site-packages (.py and data) | 105 | 14.0 | 14.1 | 14.1 | 14.1 |
| build/build-dir | 45 | 8.7 | 8.7 | 8.7 | 4.6 |
| source/tracked | 35 | 6.3 | 6.3 | 6.3 | 6.3 |
| git/loose | 0 | 0.0 | 0.0 | 0.0 | 7.1 |
| build/so (in-tree extensions) | 13 | 4.1 | 4.1 | 4.1 | 1.8 |
| installed/conda-pkgs-extracted | 14 | 0.8 | 0.8 | 0.8 | 3.1 |
| cache/other | 3 | 2.8 | 0.0 | 0.0 | 0.0 |
| build/generated-src (Cython .c) | 27 | 2.8 | 2.8 | 2.8 | 2.8 |
| other/home (e.g. `nltk_data`) | 2 | 1.0 | 1.0 | 1.0 | 1.0 |

**Reading it:**

- **`.git` is the largest delta in bytes** (240 MB mean). It costs chunks only for the first clone
  of a project, because the clones' pack files happened to be byte-identical:
  - 10 of 12 later pandas commits paid 0 MB for `.git`, and 34736 paid 11 MB;
  - pandas-63232 paid 411 MB. The 13 pandas clones produced two distinct packs, from clones about
    10 minutes apart;
  - the bare clones made on the VM held packs of 428 MB (pandas), 194 MB (scikit-learn) and
    109 MB (moto) ([`raw/mirrors.txt`](raw/mirrors.txt)).

  A multi-day C2.14 campaign will see each project's pack change whenever upstream refs move. With
  full history, later commits would then pay up to a full pack each, not the measured 30 MB.
- **Caches** are pure residue: pip wheels and sdists, and conda tarballs. They cost more on later
  pandas commits (102 MB mean) than on first tasks (54 MB), because those recipes `pip install`
  without `--no-cache-dir`.
- **Installed packages dominate what is left.**
  - Native code is the bulk: `.so` files, conda's `lib/` (MKL, LLVM, OpenBLAS), llvmlite, numpy,
    scipy, PyQt5 and pyarrow ([`raw/pkgdetail-c.txt`](raw/pkgdetail-c.txt)).
  - Of the 1,615 installs in the 29 tasks, 744 are distinct `package==version`. First installs cost
    1,172 MB.
  - The 871 repeat installs still cost 237 MB under (c), and 386 MB with today's `.pyc`. Repeats
    differ in Python ABI (`cp38` against `cp312` wheels), and today's timestamped `.pyc` never
    dedupe.
- **Source trees are cheap:** 6 MB per task. Unchanged files dedupe across commits.
- **Build artifacts are needed to run the task and are mostly unique:**
  - pandas `_libs/*.so` with their generated `.c` files;
  - meson build directories.

  `build/` and the in-tree `.so` cannot go: `import pandas` and every meson-python editable
  install (which runs `ninja` on import) need them. Only the setuptools `.o` files and debug info
  can.

## Slimming variants

New chunk MB (zstd) per task:

| Variant | n | All: mean | median | p90 | First of project: mean (median) | Later commit: mean (median) | pandas later: mean | git | installed | build | cache |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| today (base) | 29 | **275** | 216 | 569 | 279 (236) | 270 (215) | 276 | 60 | 94 | 16 | 77 |
| (a) caches, tmp, logs | 29 | **198** | 135 | 503 | 225 (181) | 168 (126) | 176 | 60 | 94 | 16 | 0 |
| (a) + `git gc` | 29 | **356** | 456 | 569 | 224 (181) | 497 (492) | 536 | 218 | 94 | 16 | 0 |
| (a) + shallow | 29 | **147** | 97 | 347 | 144 (76) | 149 (138) | 153 | 9 | 94 | 16 | 0 |
| (a) + shallow, loose objects | 29 | **145** | 98 | 343 | 146 (77) | 144 (132) | 147 | 7 | 94 | 16 | 0 |
| (a) + placeholder commit | 29 | **147** | 97 | 347 | 145 (76) | 149 (138) | 153 | 9 | 94 | 16 | 0 |
| (a) + shared project pack | 17 | (197) | (147) | (404) | 422 (526) | 149 (127) | 153 | 43 | 104 | 27 | 0 |
| **(c)** = shallow loose + pyc + strip + `.o` | 29 | **134** | 78 | 346 | 139 (73) | 128 (81) | 133 | 7 | 89 | 9 | 0 |

- **(a)** removes 77 MB per task, all of it cache. It changes nothing the task uses. One effect:
  `scikit-learn-29021`'s eval script re-runs `pip install --force-reinstall -e .[tests]`, which
  then downloads instead of using the pip cache. It still passed.
- **`git gc` is harmful.** Repacking writes a pack that is unique to each image, so later commits
  pay the whole history again (497 MB). Today's clone packs are deduplicable only because they
  arrive byte-identical.
- **Shallow, loose and placeholder** cost about the same: 6–11 MB of git per task, the compressed
  objects of one tree. Loose objects dedupe across commits: 5.6 MB per later commit against
  11.2 MB packed.
- **The shared pack** shows what full history costs when its bytes are pinned per project:
  - each later commit pays 0 MB of git, against 11 MB for shallow;
  - the first task of each project pays the whole pack (110–430 MB here).

  Its group means are not comparable to the other rows: only the 17 tasks of pandas, moto and
  scikit-learn ran this variant, so fewer packages had been seen earlier. Compare the git column.
  Overall it roughly breaks even with shallow, because 5,463 of 10,326 projects have one task. It
  is worth it only if training wants `git log` history.
- **(c) on top of shallow-loose** saves 11 MB per task:
  - checked-hash `.pyc`: installed pyc 20.6 → 13.6 MB, because repeat installs of the same source
    now dedupe;
  - `strip --strip-debug` and setuptools `.o` removal: build 16 → 9 MB.

  The pyc rewrite costs about 9 s per image at build time (42 s for xarray).
- **Slimming time** is small: 1–4 s for (a), shallow and loose, at most 12 s. The extra build cost
  is negligible.

## Verification

Per-variant results are in [`raw/verify/`](raw/verify/) and [`raw/eval/`](raw/eval/), and are
summarised in [`raw/verification.json`](raw/verification.json).

| Check | today (base) | (a) | (a)+gc | (a)+shallow / loose | (a)+placeholder | (a)+shared pack | (c) |
|---|---|---|---|---|---|---|---|
| `git status` rc 0, same porcelain output as base | 29/29 | 29/29 | 29/29 | 29/29 | **27/29** (idasen-340, pandas-43447: a build-time edit to a tracked file disappears into the commit) | 17/17 | 29/29 |
| HEAD = dataset `base_commit`, commit present | 29/29 | 29/29 | 29/29 | 29/29 | **0/29** | 17/17 | 29/29 |
| `git fsck --connectivity-only` | ok | ok | ok | ok | ok | ok | ok |
| commits reachable from HEAD | full | full | full | 1 | 1 | full | 1 |
| `git describe` / versioneer version | `x.y.dev0+N.gSHA` | same | same | `x.y.dev0` (tag re-created at HEAD) | same as shallow | same | same as shallow |
| test patch applies (`git apply --check`) | 26/29 | same | same | same | same | same | same |
| `pytest --collect-only` on the eval's targets | same count as base everywhere | = | = | = | = | = | = |
| eval script, unpatched: `OPENSWE_EXIT_CODE` | reference | – | – | identical 29/29 | identical 29/29 | – | identical 29/29 |
| eval script after the gold patch: exit 0 | **25/29** | – | – | 25/29 | 25/29 | – | 25/29 |

- **Test patches:** the three test patches that do not apply at baseline (androguard, napari-tiff,
  idasen) fail the same way in every variant. Their eval scripts tolerate it.
- **Test collection:** 0 to 529 tests were collected per task, with the same count and exit code
  in every variant. Collection segfaults for pandas-63232 and 62428, and returns errors for
  OasisLMF, xarray, fake-useragent and pandas-21799, at baseline and in every variant alike.
- **Gold-patched evals:** the same 4 tasks fail at baseline and in every variant: pandas-63232 and
  62428 (segfault), 21799, and xarray-9194. In all, 116 gold-patched and 116 unpatched evals ran,
  and every variant returned the same `OPENSWE_EXIT_CODE` as today's image for the same task. That
  includes scikit-learn-29021, whose eval does a 140 s editable reinstall.
- **What the placeholder commit breaks:**
  - eval scripts that check out or reset to the base SHA. pandas-22261 runs
    `git reset --hard <base_commit>`. Under the placeholder that fails with an unknown revision,
    the script carries on, and the test passes only because nothing had changed.
  - Across the dataset, 870 of 36,884 eval scripts name their base commit, 1,919 use `git reset`,
    1,249 `git clean`, 1,027 `git diff`, 362 `git log`/`show`/`rev-parse`/`describe`, and 90 check
    out paths from a SHA ([`raw/eval-script-git-usage.json`](raw/eval-script-git-usage.json)).
  - The shallow cut keeps all of these working except history walks (`git log`, `HEAD~1`).
- **The version string changes** under every history cut: `pandas.__version__` loses its
  `+674.g4fb963b6a3` suffix. That affects only code that parses the local version part. 43447
  reports `1.4.0.dev0+0.g805f0d9.dirty` because its build edits a tracked file. No eval result
  changed.
- **Mounted cost:** `git status` took a median of 23 ms (at most 2.6 s) in every variant. Shallow
  `.git` directories are up to 60× smaller (pandas: 441 → 7–8 MB). OasisLMF keeps 62 MB, because
  its single tree holds large data files.

## Extrapolation to 35,549 tasks

The model: 10,326 first tasks of a project, each at the "first of project" mean, plus 25,223
later commits, each at the "later commit" mean, of chunk-store bytes (zstd). Bootstraps add about
0.12 TB on top (S10: about 3.4 MB compressed per image).

| Variant | First-of-project mean MB | Later-commit mean MB | **OpenSWE TB (means)** | TB at medians | TB, later = pandas-later mean | TB, later = non-pandas later mean |
|---|---:|---:|---:|---:|---:|---:|
| today (base) | 279 | 270 | **9.7** | 7.9 | 9.8 | 8.8 |
| (a) | 225 | 168 | **6.6** | 5.1 | 6.8 | 5.4 |
| (a) + `git gc` | 224 | 497 | **14.8** | 14.3 | 15.8 | 8.9 |
| (a) + shallow | 144 | 149 | **5.3** | 4.3 | 5.3 | 4.7 |
| (a) + shallow, loose | 146 | 144 | **5.1** | 4.1 | 5.2 | 4.7 |
| (a) + placeholder | 145 | 149 | **5.3** | 4.3 | 5.4 | 4.8 |
| **(c)** | 139 | 128 | **4.7** | 2.8 | 4.8 | 3.8 |

Assumptions and how to read them:

- **The 29 tasks stand for the corpus.**
  - The 15 first-of-project tasks span 3 MB (hl7-parser) to 818 MB (OasisLMF). Most of the 5,463
    single-task projects look like the small ones, so the median column is a plausible lower
    figure.
  - Later commits are 12 pandas and 2 non-pandas (moto, scikit-learn). Pandas sits at the heavy
    end, so the two right-hand columns bracket the later-commit model.
- **Dedupe was measured on a small corpus** (11 foundations plus at most 28 earlier tasks), so the
  per-task figures are upper-leaning.
  - The installed-package class is what corpus scale shrinks. Under (c), a repeat install of an
    already-seen `package==version` costs 0.27 MB against 1.58 MB for a first install.
  - If 90% of installs at corpus scale are repeats, installed packages fall by about 27 MB per
    task, and (c) comes to **about 3.7 TB**. That is not measured.
- **Today's and (a)'s figures are optimistic for `.git`.** They assume a project's clones keep
  producing byte-identical packs, as they did within this run's 20-minute window. Over a multi-day
  campaign, every upstream push makes a new pack, and later commits pay up to the full pack (pandas
  428 MB). The history cut removes that risk.
- **Not modelled:**
  - recipes that fail today (14 of 26 pandas so far); a failed recipe stores nothing;
  - lockfile rebuilds, which could resolve dependencies toward already-stored versions.

## Recommendation for C2.14

1. **Put (c) into the OpenSWE recipe rewrite** as one final `RUN` step, the same script for every
   task. Under the chunk store, a final-step deletion is enough: only the final tree is stored. In
   OCI form it would also need squashing.
   - **Caches, /tmp and logs:** `rm -rf ~/.cache`, `conda clean -a -y`, apt lists, `/tmp`, logs.
   - **History:** cut at HEAD with `.git/shallow`, drop refs and reflogs, re-create the nearest tag
     on HEAD, unpack objects loose, and set `gc.auto=0`.
     - Keep HEAD at the dataset's `base_commit`: eval scripts `git reset --hard` to it.
     - Keep the index and the working tree untouched.
   - **Bytecode:** recompile the image's new `.pyc` as checked-hash.
   - **Native code:** `strip -p --strip-debug` in-tree `*.so`, and delete setuptools
     `build/temp.*/*.o`. Never touch a meson or ninja build directory or the in-tree `.so`; the
     tests import them.
2. **Never `git gc` or repack** in the recipe. **Don't use a placeholder commit.** Keep the
   shared-pack design in reserve, only if a training task needs real history.
3. **Budget.** (c) brings OpenSWE from about 9.7 TB to about 4.7 TB (2.8–4.8 TB across the
   models above; the median model is 2.8 TB). That is still several TB, about 65% of it installed
   dependencies. Within "not many TB", choose one or more of:
   - **Waves with a growth budget.** Precompute the training split first and stop at the budget;
     C2.14 already sets one in unique chunk bytes.
   - **Corpus-aware lockfiles.** When C2.14 records each build's resolved versions, prefer versions
     already in the store. Repeat installs cost 0.27 MB against 1.58 MB for first installs.
   - **Accept 3–5 TB** for OpenSWE as its own family budget. Every other family fits in under
     0.3 TB plus the TMax and Terminal-Lego task deltas (S10).
4. **Expect build failures** with today's unpinned recipes: 14 of 27 pandas recipes tried
   (here and in S10) fail on dependency resolution. The lockfiles matter for coverage as much as for storage.

## Run log

- **14:55:40Z:** the VM was created with the prescribed `hz.py server` command, after clearing the
  gateway's `known_hosts` entry for 10.42.0.43.
- **14:56–15:00:** 11 foundation pulls and walks (about 26 s per 2.2 GB tree), plus the bare clones
  for the shared pack.
- **15:00–15:40:** 34 builds, with 5 or 6 in parallel (median 156 s, longest 759 s for
  pandas-21799), variant derivation and the walks (220 walks, median 22 s).
- **Two script bugs, fixed in place:**
  - mounting the pyc list inside a read-only bind mount;
  - `docker commit -q`, which Docker 29 does not support.

  The fixes re-created the variant images with the same script. The walks come from the first
  derivation.
- **15:42–16:10:** verification and both eval passes, the package detail and the Nydus
  calibration.
- **Cleanup:** the gateway staging files (`/root/openswe-deltas-*`) and the gateway's
  `known_hosts` entry for 10.42.0.43 were removed. Then the VM was deleted, at 16:10:25Z, with
  `hz.py delete-server`. Afterwards the Hetzner API listed no server named
  `sandboxes-spike-openswe`.

## Files

- [`raw/results.json`](raw/results.json): per task and variant, delta bytes, files and new chunk
  bytes (uncompressed and zstd) per subclass, plus the top directories of new chunks.
- [`raw/summary.json`](raw/summary.json): the aggregates above
  ([`scripts/summarize.py`](scripts/summarize.py)); [`scripts/tables.py`](scripts/tables.py)
  prints the tables.
- [`raw/verify/`](raw/verify/), [`raw/eval/`](raw/eval/),
  [`raw/verification.json`](raw/verification.json): per-variant harness checks and eval outcomes
  (output tails, which contain no patches).
- [`raw/pipeline.jsonl`](raw/pipeline.jsonl): every build, slim, walk, verify and eval step with
  its timing and errors.
- [`raw/pkgdetail-base.txt`](raw/pkgdetail-base.txt),
  [`raw/pkgdetail-c.txt`](raw/pkgdetail-c.txt): new chunk bytes by installed package, and the cost
  of repeat installs.
- [`raw/calibration.json`](raw/calibration.json): the walker against `nydus-image`.
  [`raw/mirrors.txt`](raw/mirrors.txt): the per-project bare-clone pack sizes.
- [`raw/tasks.json`](raw/tasks.json), [`raw/order.json`](raw/order.json): recipes and processing
  order. [`raw/eval-script-git-usage.json`](raw/eval-script-git-usage.json): git usage across all
  36,884 eval scripts.
- [`scripts/`](scripts/): what ran.
  - [`select.py`](scripts/select.py): task selection.
  - [`evalscan.py`](scripts/evalscan.py), [`prep_eval.py`](scripts/prep_eval.py): eval
    definitions.
  - [`pipeline.py`](scripts/pipeline.py): the VM driver.
  - [`slim.sh`](scripts/slim.sh): the variants, which is the recipe step to adopt.
  - [`walk.py`](scripts/walk.py), [`analyze.py`](scripts/analyze.py): accounting.
  - [`verify.sh`](scripts/verify.sh), [`run_eval.sh`](scripts/run_eval.sh): verification.
  - [`calib.sh`](scripts/calib.sh), [`calib.py`](scripts/calib.py), [`rafs.py`](scripts/rafs.py):
    calibration.

  These are spike code.
