# M1 gate run (2026-10-03)

The chunk store's M1 gate ([design §9](../../chunk-store-design.md#9-build-plan)) on S10's 181-image sample, run by `scripts/chunk_store_gate.py`.

**Result: fail.** Run `20261003t1910`, S3 prefix `spike/m1/20261003t1910`.

| Criterion | Status | Measured | Gate |
| --- | --- | --- | --- |
| full_tree | pass | 181/181 | 181/181 equal (contents, modes, owners, mtimes, xattrs) |
| stored_bytes | fail | 20.62 GB | 17.5 GB +-5% |
| cold_commands | pass | 20/20 within limit, 1 not in the image | <= 1.3x S10 (pip: <= 1.3x S12 local, else 2.5 s), trace replayed |
| burst_20 | fail | 6.07 s (20 sandboxes) | <= 5.5 s (S11) |
| crash_injection | pass | {"failed_steps": [], "steps": 12} | every write-path step: killed, no visible partial image, rerun converges |
| rollback_10 | pass | 10/10 | 10 images unpacked, tree-equal and rebuilt by today's builder |

## Resources

8.49 VM-hours, about EUR 2.29 at list price (billed per started hour).

| Server | Type | Hours | Billed |
| --- | --- | ---: | ---: |
| sandboxes-m1-20261003t1910-b1 | ccx43 | 0.87 | 1 |
| sandboxes-m1-20261003t1910-converter | ccx63 | 2.89 | 3 |
| sandboxes-m1-20261003t1910-store | ccx43 | 2.9 | 3 |
| sandboxes-m1-20261003t1910-w1 | ccx43 | 0.94 | 1 |
| sandboxes-m1-20261003t1910-w2 | ccx43 | 0.89 | 1 |

## Cold first commands (traced)

| Image | Command | Wall s | Limit s | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 0 | import_sys | 0.349 | 0.598 | 0.76 |
| 0 | git_status | 0.036 | 0.325 | 0.14 |
| 0 | pip_version | 0.951 | 2.327 | 0.53 |
| 60 | import_sys | 0.223 | 0.559 | 0.52 |
| 60 | git_status | 0.085 | 0.403 | 0.27 |
| 60 | pip_version | 0.921 | 2.327 | 0.51 |
| 63 | import_sys | 0.198 | 0.533 | 0.48 |
| 63 | git_status | 0.102 | 0.65 | 0.2 |
| 63 | pip_version | 0.999 | 2.431 | 0.53 |
| 72 | import_sys | 0.171 | 0.65 | 0.34 |
| 72 | git_status | 0.075 | 0.676 | 0.14 |
| 72 | pip_version | 0.187 | - | not in image |
| 90 | import_sys | 0.181 | 0.468 | 0.5 |
| 90 | git_status | 0.05 | 0.338 | 0.19 |
| 90 | pip_version | 0.846 | 2.5 | - |
| 130 | import_sys | 0.141 | 0.52 | 0.35 |
| 130 | git_status | 0.025 | 0.338 | 0.1 |
| 130 | pip_version | 0.515 | 2.5 | - |
| 170 | import_sys | 0.167 | 0.52 | 0.42 |
| 170 | git_status | 0.034 | 0.338 | 0.13 |
| 170 | pip_version | 0.797 | 2.5 | - |

## 20-way burst against today's path

The same bench, images and worker type: the chunk store against the live path (no chunk store).

| Path | Mode | Wall s | Create p50 / max | import sys p50 / max | pip p50 / max |
| --- | --- | ---: | ---: | ---: | ---: |
| chunk_store | demand | 10.142 | 3.234 / 8.829 | 0.33 / 3.33 | 1.506 / 2.455 |
| chunk_store | traced | 6.068 | 3.439 / 4.76 | 0.292 / 0.478 | 1.332 / 1.81 |
| today | demand | 11.307 | 3.677 / 7.112 | 1.399 / 2.35 | 4.748 / 6.285 |
| today | traced | 9.567 | 3.511 / 6.543 | 0.944 / 2.059 | 4.306 / 6.043 |

Raw results: `summary.json` here, and `build/m1-gate/<run>/raw/` on the operator machine.

## Reading it

Run 3 tests C2.1's decision on the production path:
- **Converters** wrote `--nydusd-blobs`, with chunk reservations (`820a418`).
- **Canaries** served RAFS through the bundle-pinned config switch
  (`chunk_store.nydusd`, 4 nydusd threads, one shared cache) and attached 8 at
  once.
- **The baseline worker** ran production's 0.8.6 bundle.
- **All gate workers stayed unregistered** (`gateway_port: 1`).

**What happened along the way:**
- **The workers phase ran three times.**
  - Production's new `direct_local_model_waits` key made the old canary bundle
    reject its config, so the canary bundle was rebuilt from `bd3df58` on 0.8.6.
  - The gate's nydusd install copied into a directory that did not yet exist
    (fixed).
  - Then a stall, below.
- **Conversion and crash injection** are unaffected.
- **Rerun benches are warmer:** after the stall, the sequential and burst
  benches were rerun on a store node that was partly warm.

**Correctness passes again:** 181/181 full tree, all 12 crash steps, rollback
10/10, and every cold first command well inside its limit.

**Stored bytes are not valid this run:** 20.62 GB, against 17.5 GB ±5%.
- **A gate-driver bug.** The gate gave all 12 parallel converters one owner, so
  the index treated them as one builder: layer claims and chunk reservations
  never separated them. 32 layers were converted twice, and no converter ever
  waited on a reservation.
- **Fixed in `988e19d`**, with one owner per converter slot.
- **Without the duplicates, it would pass.** Live chunks are 16.27 GB in
  19.39 GB of packs. Live chunks plus bootstraps, maps and tails (1.13 GB) come
  to about 17.4 GB, inside the gate. This is nydus's own chunk encoding
  (`--nydusd-blobs`), not S12's re-encoded packs.

**The burst: the chunk store now beats today's path in both modes.**
- **Traced:** 6.07 s against 9.57 s (0.63×). Run 2 was 0.87×.
- **Demand:** 10.1 s against 11.3 s (0.90×). Run 2 was 1.49×: its Python
  reader's attach was the bottleneck, and nydusd removes it.
- **First commands are 3–4× faster:** `import sys` p50 0.29–0.33 s against
  0.94–1.40 s, and `pip` 1.3–1.5 s against 4.3–4.7 s.
- **The absolute 5.5 s criterion still fails.** It is S11's number from
  another harness. The M2 precondition, which matters for migrating, is
  traced within 1.3× of today's path, and this run is at 0.63×.

**The stall: a finding.**
- **What workers saw.** On the first bench attempt, every nydusd read on both
  canaries failed for about 4.5 minutes. NBD timed out after 60 s, EROFS
  returned EIO, and the backend fenced the image, as designed.
- **It was not S3.** S3 had 12 errors in 5,399 requests, and direct reads were
  healthy.
- **It was the chunk index.** Workers' locator calls and the store node's
  per-blob `locate` calls (for nydusd's virtual blobs) computed chunk
  locations in one Python process:
  - one 70,000-entry locator takes 0.7 s;
  - 20 at once took 44 s;
  - clients time out at 30–60 s, and nydusd retried every 30 s.
- **Fixed in `b9e604e`.** Registration now stores each locator and each blob's
  layout, so reads never query the index.

**Next, run 4.** With distinct converter owners, stored locators and the
bundle-pinned nydusd (`6d322bc`):
- stored bytes must land near 17.4 GB;
- a cold 20-image burst must not stall the index.
