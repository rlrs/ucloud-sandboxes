# M1 gate run (2026-10-04)

The chunk store's M1 gate ([design §9](../../chunk-store-design.md#9-build-plan)) on S10's 181-image sample, run by `scripts/chunk_store_gate.py`.

**Result: fail.** Run `20261003t2214`, S3 prefix `spike/m1/20261003t2214`.

| Criterion | Status | Measured | Gate |
| --- | --- | --- | --- |
| full_tree | pass | 181/181 | 181/181 equal (contents, modes, owners, mtimes, xattrs) |
| stored_bytes | pass | 17.61 GB | 17.5 GB +-5% |
| cold_commands | fail | 19/20 within limit (over: 0:import_sys), 1 not in the image | <= 1.3x S10 (pip: <= 1.3x S12 local, else 2.5 s), trace replayed |
| burst_20 | fail | 7.50 s (20 sandboxes) | <= 5.5 s (S11) |
| crash_injection | pass | {"failed_steps": [], "steps": 12} | every write-path step: killed, no visible partial image, rerun converges |
| rollback_10 | pass | 10/10 | 10 images unpacked, tree-equal and rebuilt by today's builder |

## Resources

9.71 VM-hours, about EUR 2.9 at list price (billed per started hour).

| Server | Type | Hours | Billed |
| --- | --- | ---: | ---: |
| sandboxes-m1-20261003t2214-converter | ccx63 | 3.68 | 4 |
| sandboxes-m1-20261003t2214-store | ccx43 | 3.69 | 4 |
| sandboxes-m1-20261003t2214-w1 | ccx43 | 0.8 | 1 |
| sandboxes-m1-20261003t2214-w2 | ccx43 | 0.78 | 1 |
| sandboxes-m1-20261003t2214-b1 | ccx43 | 0.76 | 1 |

## Cold first commands (traced)

| Image | Command | Wall s | Limit s | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 0 | import_sys | 0.699 | 0.598 | 1.52 |
| 0 | git_status | 0.061 | 0.325 | 0.24 |
| 0 | pip_version | 2.13 | 2.327 | 1.19 |
| 60 | import_sys | 0.407 | 0.559 | 0.95 |
| 60 | git_status | 0.164 | 0.403 | 0.53 |
| 60 | pip_version | 2.023 | 2.327 | 1.13 |
| 63 | import_sys | 0.355 | 0.533 | 0.87 |
| 63 | git_status | 0.175 | 0.65 | 0.35 |
| 63 | pip_version | 2.007 | 2.431 | 1.07 |
| 72 | import_sys | 0.356 | 0.65 | 0.71 |
| 72 | git_status | 0.168 | 0.676 | 0.32 |
| 72 | pip_version | 0.396 | - | not in image |
| 90 | import_sys | 0.354 | 0.468 | 0.98 |
| 90 | git_status | 0.062 | 0.338 | 0.24 |
| 90 | pip_version | 1.839 | 2.5 | - |
| 130 | import_sys | 0.272 | 0.52 | 0.68 |
| 130 | git_status | 0.041 | 0.338 | 0.16 |
| 130 | pip_version | 1.17 | 2.5 | - |
| 170 | import_sys | 0.341 | 0.52 | 0.85 |
| 170 | git_status | 0.062 | 0.338 | 0.24 |
| 170 | pip_version | 1.482 | 2.5 | - |

## 20-way burst against today's path

The same bench, images and worker type: the chunk store against the live path (no chunk store).

| Path | Mode | Wall s | Create p50 / max | import sys p50 / max | pip p50 / max |
| --- | --- | ---: | ---: | ---: | ---: |
| chunk_store | demand | 11.81 | 4.067 / 8.998 | 0.45 / 3.995 | 2.101 / 2.791 |
| chunk_store | traced | 7.5 | 4.416 / 5.707 | 0.317 / 0.551 | 1.663 / 1.977 |
| today | demand | 12.889 | 4.447 / 8.138 | 1.554 / 2.666 | 5.124 / 6.83 |
| today | traced | 9.9 | 3.729 / 6.87 | 1.02 / 1.955 | 4.526 / 5.944 |

Raw results: `summary.json` here, and `build/m1-gate/<run>/raw/` on the operator machine.

## Reading it

Run 4 tests three fixes from run 3:
- one index owner per converter slot (`988e19d`);
- locators and blob layouts stored at registration (`b9e604e`);
- nydusd from the node bundle, with its cache inside `cache_bytes`.

**Along the way:**
- **Two of 157 conversions failed twice on S3 read timeouts in the index.**
  - Registration read each blob's whole tail object. The index's presigned S3
    reads had no retry, and the converter gave up on registration after 60 s.
  - Fixed in `e75aae3` and `c016bab` (0.9.1): range reads of the tail table,
    retries on transport errors and 5xx, and a 600 s registration timeout.
  - The gate's store node and converter were patched in place with that
    `chunk_index.py`, and conversion then completed.
- **The workers phase refused the baseline config once.** Since 0.9.0, the live
  config carries production's own chunk store, and the baseline copy now drops
  it (`31ce908`).

**What passes:**
- Stored bytes: 17.61 GB, within the gate. Run 3's 20.62 GB was the shared
  converter owner.
- Full tree 181/181, all 12 crash steps, and rollback 10/10.

**The burst: still faster than today's path.**
- Traced: 7.50 s against 9.90 s (0.76×). Demand: 11.8 s against 12.9 s
  (0.92×).
- First commands are 3× faster: `import sys` p50 0.32 s against 1.02 s, and
  `pip` 1.66 s against 4.53 s.
- M2's precondition (traced within 1.3× of today's path) holds. S11's absolute
  5.5 s does not, as in run 3.

**Cold commands miss by one.** Image 0's `import sys` took 0.70 s against a
limit of 0.60 s; run 3 measured 0.35 s for the same command.
- The gate measures cold commands against a cold store node, so each first
  read waits on S3.
- M2 wave 1 showed that Hetzner S3 stalls about 2.4% of GETs for 6–60 s
  (below).
- In production, an image is switched only once its store-node objects are
  warm (0.9.3), so its first commands never wait on S3.

**Verdict.** Correctness and stored bytes pass, and the burst is faster than
today's path. The remaining misses measure S3's tail on a cold store node,
which M2 avoids by warming before a switch. M2 proceeds: wave 1 is in
production ([chunk-store-m2-plan.md](../../chunk-store-m2-plan.md), "Wave 1").
