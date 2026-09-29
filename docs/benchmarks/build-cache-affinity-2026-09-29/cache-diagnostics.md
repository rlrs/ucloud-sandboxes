# BuildKit cache-import diagnostics

Affinity selection found all 48 exact inputs in the representative repeat, but
many application instructions still executed. The repeat finished faster as a
batch, while its individual build/push p95 increased; see
[the performance comparison](performance-comparison.md).

Input drift is not supported by the retained evidence. A local two-extraction
audit preserved file contents, modes, ownership, sizes and file mtimes across
2,017 entries; only directory times and ctimes changed
([receipt](context-materialization-audit.json)). More decisively, the exact
seed/repeat case001 cache metadata had identical record digests, input links and
selectors across all 13 graph records, including the final COPY and RUN
([comparison](cache-record-comparison.json)). Different result blobs or result
creation times do not by themselves mean the build graph changed.

## First diagnostic: one versus eight versus 64 imports

The [first diagnostic receipt](cache-concurrency-diagnostic-r1.json) completed
from 13:41:26.812 to 13:45:04.040 UTC on 2026-09-29. It used the same frozen
latest-64 tag/digest inventory for all arms, pinned every cache import by manifest
digest, and checked the copied contexts against the original seed content hashes.
Each arm had an independent empty BuildKit daemon configured with the same four
solver and four registry-request limits as production. It exported neither cache
manifests nor images; the output mode was `cacheonly`.

| Arm | Requests | Concurrent solves | Imports per solve | Application execution observations | Cached observations | Arm wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Serial, exact manifest | 1 | 1 | 1 | 0 | 1 | 1.569 |
| Shared daemon, exact-first eight | 12 | 4 | 8 | 6 | 6 | 73.380 |
| Shared daemon, common 64 | 12 | 4 | 64 | 12 | 0 | 120.306 |

There were zero cache-import error vertices. All three private daemons were
removed with zero cleanup errors, and the frozen cache snapshot still matched at
the end. Broader imports did **not** solve the missing reuse in this experiment;
increasing the production import limit is not justified by these results.

The serial exact case proves that a stored application result is loadable. R1
alone does not isolate the effect of importing multiple caches from solve
concurrency: the first arm changes both at once, and its particular source
variant was already a hit in the concurrent-eight arm. R2 therefore added serial
eight-import checks and concurrent exact-one checks with shared and isolated
daemons.

These are per-request progress observations. BuildKit can display shared
concurrent vertices in multiple clients, so they are not counts of unique
physical command executions. Numeric command output is treated as execution
evidence even when the vertex also materializes layers; missing or ambiguous
application progress fails the diagnostic. `cacheonly` verifies cache resolution
and build success, not the contents of a runnable exported image. Registry/host
page caches may warm between arms, and the arm timings are diagnostic rather
than a production batch-speed forecast.

The second helper, `scripts/qualify_buildkit_cache_isolation.py`, replays the
first receipt's immutable imports rather than selecting newer live tags. It
requires matching fixture hashes, BuildKit image and configuration; validates
that frozen manifests remain available; and cleans only its own drivers.

## Second diagnostic results

The [second receipt](cache-isolation-diagnostic-r2.json) passed from
13:53:44.497 to 13:55:10.519 UTC. Its recorded R1 input SHA-256 matches the
archived R1 receipt byte for byte; all 64 frozen inventory entries and all twelve
case/import plans are unchanged. Configuration and pinned BuildKit image also
match. Every arm started with fresh private BuildKit stores.

| Arm | Requests | Daemons | Concurrent solves | Imports per solve | Application execution observations | Cached observations | Arm wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Serial variant5, previously an R1 hit | 1 | 1 | 1 | 8 | 1 | 0 | 32.282 |
| Serial variant7, previously an R1 miss | 1 | 1 | 1 | 8 | 1 | 0 | 31.829 |
| Shared daemon, exact cache only | 12 | 1 | 4 | 1 | 0 | 12 | 3.620 |
| Four exclusive daemons, exact cache only | 12 | 4 | 4 total | 1 | 0 | 12 | 3.226 |

All seven private drivers were removed with zero cleanup errors; no import-error
vertices were observed. Every frozen manifest remained available after the arms.

**Importing multiple caches is sufficient to reproduce a miss here without
concurrent solves.** Both serial eight-import cases executed. Conversely, all
twelve exact-only builds reused the application result while sharing one daemon
at concurrency four. Four daemons produced the same twelve cached observations;
this evidence does not require a daemon-per-slot production architecture. The
variant5 result changed between its R1 and R2 eight-import attempts, so this is
not a claim that a given multi-import build deterministically misses every time.

The cache-record cross-check identifies a plausible mechanism: a multistage cache
can contain the same keys as a tools cache while omitting the final tools result
layers. BuildKit's combined-cache lookup may retain one manager's version of a
duplicate key and consult only that manager's results, allowing an incomplete
record to obscure a complete result elsewhere. The retained
[source references and graph comparison](cache-record-comparison.json) support
that explanation. The diagnostics demonstrate the importer-policy effect;
they do not establish a specific upstream issue as the cause or claim that an
upstream BuildKit bug has been patched.

These results justify qualifying a narrow candidate: offer only the exact cache
when the verified affinity selector finds one, while retaining normal fallback
selection when it does not. BuildKit must continue validating all inputs and
publishing through the existing image path. **The 3.620-second result is not a
production build-batch result**: it uses `cacheonly`, omits image/cache export and
environment publication, has twelve requests rather than 48, and does not execute
sandbox smoke checks. A new full pipeline qualification is required before
claiming a corresponding production speedup.

## Second diagnostic protocol

The frozen helper SHA-256 is
`cd046d289ad849381db870b4b3deb8571e7e21de95239cc88fac26ca18e826ae`.
It adds four arms, each starting with fresh private stores:

1. Serial exact-eight imports for variant5, an R1 hit, on its own daemon.
2. Serial exact-eight imports for variant7, an R1 miss, on another daemon.
3. All twelve contexts with only their exact manifest, four concurrent solves
   on one daemon.
4. The same twelve exact-manifest builds on four private daemons. One exclusive
   sequential job stream owns each daemon, so no daemon runs concurrent solves.
   Round-robin case-index streams are `[1,13,25]`, `[4,16,28]`, `[7,19,31]`, and
   `[10,22,34]`; each daemon can warm across its three jobs.

The second command uses the same registry, contexts, seed-selection receipt,
configuration, and candidate `PYTHONPATH` as R1, with script
`qualify_buildkit_cache_isolation.py` and these additional/replaced arguments:

```sh
--frozen-receipt /work/cache-concurrency-diagnostic-r1/receipt.json \
--work-root /work/cache-isolation-diagnostic-r2
```

R1's helper and receipt are unchanged. R2 creates seven private drivers in total,
at most four simultaneously, and removes each after its arm or in final cleanup.
It records the driver responsible for every result. Its cache-only export mode
and progress limitations are the same as R1; it cannot substitute for the later
SDK image/sandbox semantic qualification.

## Final resource audit

The [final audit](final-state.json) passed at 13:58:21 UTC after the repeat pool's
13:55:36 release. All ten unique IDs in the [resource ledger](resource-ledger.json)
were absent at the provider; fleet, sandbox and active-build counts were zero;
reservations were absent; the sampler was inactive; and gateway, relay and metrics
health passed with idle relay/lifecycle queues. All 96 exact build-history records
were successful and the three required application smokes passed and were deleted.
The ten R1/R2 private BuildKit drivers and the earlier three controlled-proof
drivers had already been removed by their respective diagnostic receipts.
