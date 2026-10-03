# M1 gate run (2026-10-03)

The chunk store's M1 gate ([design §9](../../chunk-store-design.md#9-build-plan)) on S10's 181-image sample, run by `scripts/chunk_store_gate.py`.

**Result: fail.** Run `20261003t0903`, S3 prefix `spike/m1/20261003t0903`.

| Criterion | Status | Measured | Gate |
| --- | --- | --- | --- |
| full_tree | pass | 181/181 | 181/181 equal (contents, modes, owners, mtimes, xattrs) |
| stored_bytes | fail | 20.05 GB | 17.5 GB +-5% |
| cold_commands | pass | 20/20 within limit, 1 not in the image | <= 1.3x S10 (pip: <= 1.3x S12 local, else 2.5 s), trace replayed |
| burst_20 | fail | 9.70 s (20 sandboxes) | <= 5.5 s (S11) |
| crash_injection | pass | {"failed_steps": [], "steps": 12} | every write-path step: killed, no visible partial image, rerun converges |
| rollback_10 | pass | 10/10 | 10 images unpacked, tree-equal and rebuilt by today's builder |

## Resources

8.4 VM-hours, about EUR 2.9 at list price (billed per started hour).

| Server | Type | Hours | Billed |
| --- | --- | ---: | ---: |
| sandboxes-m1-20261003t0903-converter | ccx63 | 3.01 | 4 |
| sandboxes-m1-20261003t0903-store | ccx43 | 3.02 | 4 |
| sandboxes-m1-20261003t0903-w1 | ccx43 | 0.82 | 1 |
| sandboxes-m1-20261003t0903-w2 | ccx43 | 0.79 | 1 |
| sandboxes-m1-20261003t0903-b1 | ccx43 | 0.76 | 1 |

## Cold first commands (traced)

| Image | Command | Wall s | Limit s | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 0 | import_sys | 0.082 | 0.598 | 0.18 |
| 0 | git_status | 0.014 | 0.325 | 0.06 |
| 0 | pip_version | 0.452 | 2.327 | 0.25 |
| 60 | import_sys | 0.058 | 0.559 | 0.13 |
| 60 | git_status | 0.03 | 0.403 | 0.1 |
| 60 | pip_version | 0.405 | 2.327 | 0.23 |
| 63 | import_sys | 0.058 | 0.533 | 0.14 |
| 63 | git_status | 0.043 | 0.65 | 0.09 |
| 63 | pip_version | 0.457 | 2.431 | 0.24 |
| 72 | import_sys | 0.064 | 0.65 | 0.13 |
| 72 | git_status | 0.029 | 0.676 | 0.06 |
| 72 | pip_version | 0.061 | - | not in image |
| 90 | import_sys | 0.051 | 0.468 | 0.14 |
| 90 | git_status | 0.017 | 0.338 | 0.07 |
| 90 | pip_version | 0.434 | 2.5 | - |
| 130 | import_sys | 0.052 | 0.52 | 0.13 |
| 130 | git_status | 0.012 | 0.338 | 0.05 |
| 130 | pip_version | 0.278 | 2.5 | - |
| 170 | import_sys | 0.049 | 0.52 | 0.12 |
| 170 | git_status | 0.023 | 0.338 | 0.09 |
| 170 | pip_version | 0.322 | 2.5 | - |

## 20-way burst against today's path

The same bench, images and worker type: the chunk store against the live path (no chunk store).

| Path | Mode | Wall s | Create p50 / max | import sys p50 / max | pip p50 / max |
| --- | --- | ---: | ---: | ---: | ---: |
| chunk_store | demand | 21.067 | 11.298 / 19.294 | 0.336 / 0.836 | 1.622 / 2.471 |
| chunk_store | traced | 9.696 | 4.554 / 8.682 | 0.402 / 0.652 | 2.365 / 3.285 |
| today | demand | 14.109 | 4.872 / 8.336 | 1.819 / 2.925 | 5.252 / 7.906 |
| today | traced | 11.136 | 3.436 / 7.068 | 1.268 / 2.033 | 4.716 / 6.73 |

Raw results: `summary.json` here, and `build/m1-gate/<run>/raw/` on the operator machine.

## Reading it

Run 2 repeats run 1 ([m1-gate-20261002t2212](../m1-gate-20261002t2212/README.md))
with the fixes since:
- per-pack commits (`3589345`);
- byte-exact rollback (`6431144`);
- path-ordered layers and the node-routed, warmed verifier, built into this
  run's bundle rather than patched in;
- a baseline worker `b1` on today's path: the live config and the 0.8.4
  bundle, no chunk store.

**What passes, and the misses:**
- **Correctness passes:** 181/181 full tree, crash injection at all 12 steps,
  and rollback **10/10**. Run 1 had 8/10, with the symlink `.` components now
  fixed.
- **Cold commands pass:** 20/20, with image 72's `pip` reported as not in the
  image.
- **Stored bytes still fail, at 20.05 GB.**
  - Run 1 stored 22.89 GB. Live chunks are about 16.6 GB, within 5% of
    17.5 GB.
  - Dead bytes fell from 5.1 GB to about 3.4 GB, about 2.3 GB of it from the
    12-way convert pass.
  - Per-pack commits narrowed the window to one pack's fill and upload. With
    12 converters starting together on images that share files, that still
    leaves duplicates.
  - Remaining options:
    1. reserve in-flight chunk ids in the index (exact, but a builder that
       dies leaves a reservation to expire);
    2. smaller packs, which narrow the window;
    3. let compaction (M4) reclaim the dead bytes.
- **The 20-way burst fails S11's 5.5 s** (9.70 s), but S11 was not the same
  harness.

**Same bench, same 20 images, same worker type:**

| path | mode | wall, s | create p50 / max, s | `import sys` p50, s | `pip` p50, s |
| --- | --- | ---: | ---: | ---: | ---: |
| chunk store | traced | **9.70** | 4.6 / 8.7 | **0.40** | **2.37** |
| today (b1, 0.8.4) | traced | 11.14 | 3.4 / 7.1 | 1.27 | 4.72 |
| chunk store | demand | 21.07 | 11.3 / 19.3 | 0.34 | 1.62 |
| today (b1, 0.8.4) | demand | **14.11** | 4.9 / 8.3 | 1.82 | 5.25 |

- **Traced: the chunk store is faster.** Wall time is 0.87× today's, and first
  commands are 2–3× faster. That meets the M2 precondition (traced within
  1.3× of today's path, docs/chunk-store-m2-plan.md §1).
- **Demand: the chunk store's first commands are still faster,** but its
  creates are about 2.3× slower, so wall time is 1.49× today's.
  - In demand mode each attach fetches its RAFS bootstrap and chunk map through
    the store node, partly cold from S3.
  - Shared startup traces (`2dac20a`) put a fresh node in the traced case. They
    do not speed up the attach.

**Next:**
- profile the RAFS attach (bootstrap and map fetch, and mount) under a demand
  burst;
- decide the stored-bytes option before M2's parallel conversion.
