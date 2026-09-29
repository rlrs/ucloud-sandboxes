# Controlled cache-selection proof

The controlled proof **passed** on owned seed builder `167966880` after the
48-build seed phase and telemetry tail, from 13:20:40.102 to 13:21:24.955 UTC on
2026-09-29. The [receipt](controlled-proof.json) reports all four semantic arms
passed, three owned drivers removed, ten owned cache manifests deleted, and zero
cleanup errors. It is a semantic/selection proof, not a batch latency benchmark.

| Arm | Target RUN | Expected proof-file bytes | Observed wall seconds |
| --- | --- | --- | ---: |
| Recency baseline, empty store | Executed | Passed | 6.244 |
| Exact affinity, independent empty store | Cached | Identical to baseline | 5.479 |
| Changed source, stale imports | Executed | New source verified | 2.070 |
| Changed consumed ARG, stale imports | Executed | New ARG verified | 1.468 |

Both main arms imported eight caches from the same frozen ten-manifest inventory.
The exact early result was outside baseline's eight and first in affinity's
eight. The last two arms reuse the candidate's local store and intentionally
offer stale imports; their times are not fresh-store performance comparisons.
The tiny instruction and base-image materialization make all four timings
unsuitable for predicting real application speedups.

For reproduction, run only after the owned seed phase and telemetry tail on a
retained idle builder, using a new work directory:

Stage both `scripts/qualify_build_cache_affinity.py` and
`scripts/analyze_buildkit_progress.py` into the same private directory. Run as
the builder's root service user with the deployed candidate package selected:

```sh
PYTHONPATH=/var/cache/ucloud-sandboxes/init-packages/82a8fd3da97b8a5ff0d4d5f9eff02fe60ff8bd22a5305f278a8a353b582cb008/agent-runtime/site-packages \
  /usr/bin/python3 /work/cache-affinity-proof-tools/qualify_build_cache_affinity.py \
  --registry-url http://10.42.0.2:5000 \
  --base-image node:22-bookworm-slim@sha256:43ac6c60b8f89723f746e8a92ce91abd5017e627ce1ddfe4238355d3a30b772c \
  --buildkit-config /etc/ucloud-sandboxes/buildkit/buildkitd.toml \
  --work-root /work/cache-affinity-proof-r1 \
  --timeout-seconds 900
```

The base pin is the smallest existing frozen fixture base from
[`../build-load-2026-09-29/base-pins.json`](../build-load-2026-09-29/base-pins.json).
The helper uses the candidate's pinned BuildKit image and existing configuration,
checking solver concurrency and registry concurrency are both four. It needs
Python 3.11 or newer; the qualified builders run 3.14. It does not change the
selected/shared BuildKit builder or Docker configuration.

The helper creates ten tiny source variants of one Dockerfile and exports each
using `mode=min` to an invocation-owned `ucloud-build-cache-qual-<random>`
repository. Tags use real creation timestamps. If needed, it waits until the
first seed's creation second ends before making the other nine, so that variant
zero is unambiguously outside the newest eight. There are no manufactured
timestamps or modifications to existing cache tags.

It freezes all ten tag/digest identities and compares two independent, empty
BuildKit drivers. The baseline imports the latest matching recipe and recent
fallbacks without an affinity hint. Because the fixture has one recipe, this is
the historical selection algorithm over the same inventory. The candidate
imports the exact old source result first. Both import exactly eight caches.
Neither trial exports a new registry cache, so the frozen inventory is unchanged.

Acceptance requires all of the following:

- The exact early cache tag is absent from baseline's imports and first in the
  affinity imports; all frozen manifest digests still match after the trials.
- Baseline emits an explicit marker from inside the target `RUN`; affinity has a
  `CACHED` marker for that instruction and does not execute the marker.
- Both exported root files contain the exact expected source bytes, ARG value,
  and source SHA-256. Baseline and affinity outputs match byte for byte for these
  three proof files. Rootfs tar archives are inspected without extraction and
  removed after verification.
- A changed source consumed by `COPY` and a changed ARG consumed by `RUN` both
  execute the instruction and produce their new expected bytes, even when
  deliberately offered the old cache imports.
- All three invocation-owned drivers and all exact owned cache manifests are
  removed; post-cleanup tag checks find no owned references.

`receipt.json` records the selection, input hashes, sanitized progress, output
file hashes, timings, and cleanup. Raw logs stay in the private work directory;
archive only the receipt. `complete: true` requires both proof success and clean
resource removal. A baseline that unexpectedly reuses the result is explicitly
inconclusive and fails rather than claiming an improvement.

Deletion is limited to manifest digests resolved from the helper's exact tags in
its random repository. It never prunes another cache, removes the shared builder,
or runs registry GC. Unreferenced registry blobs await normal GC. Driver removal
also terminates any owned build still running after a timeout. The overall build
deadline is bounded; cleanup has a separate allowance so it can still run after
the deadline. An interrupted/error run retains its receipt and cleanup errors.

This proof isolates selection and BuildKit's own invalidation. The helper's
synthetic affinity hash does not test the gateway's propagation of immutable
context/request identities; separate runtime tests cover that integration. It
also does not show that an edited application can reuse an old application
instruction, or predict the latency of the representative 48-request burst.
