# Warm-cache tail analysis

The 24-build warm phase succeeded, but it was a mixture of complete hits,
dependency-cache hits followed by application rebuilds, and waits for concurrent
build work. Its client p50 was 14.466 s and p95 was 61.650 s. A `CACHED` line for
one compile vertex does not establish that the request avoided compilation.

This analysis uses [warm results](warm/summary.json),
[cold results](cold/summary.json),
[the image inventory](image-inventory-after-warm.json), and sanitized remote
analysis of the owned benchmark's retained logs and small OCI layers. Remote
inspection returned only step categories, timings, cache correlations, aggregate
metadata comparisons and hashes. No raw logs, source contents or credentials
were copied into this artifact. No production changes were made.

## Measured: Python warm-003 and warm-021

| Case | Variant | Builder job | Build/push | EROFS phase | Docker pull within EROFS |
| --- | --- | --- | ---: | ---: | ---: |
| `bl20260929-warm-003` | `app-change-2` | `167927367` | 21.417 s | 35.797 s | 33.748 s |
| `bl20260929-warm-021` | `app-change-4` | `167927367` | 21.399 s | 35.801 s | 33.741 s |

That builder ran Node cases during the cold phase, not Python. Both warm cases
reported five reused EROFS groups and one newly built 5,345,280-byte group.

Retained BuildKit progress establishes the following for both requests:

- The dependency installation and native-extension installation were `CACHED`.
- Materializing the cached native/rootfs vertex took 10.8 s and emitted download
  progress. This is cached-result materialization, not another pip installation.
- `COPY src` ran for 2.9 s, followed by the uncached Python compile/smoke command
  for 3.5 s. Image export took 1.4 s for warm-003 and 1.0 s for warm-021; cache
  export took 1.1 s. These progress durations can overlap and are not additive
  replacements for the measured phase timer.
- The subsequent Docker materialization cost dominated the EROFS phase, even
  though only the small final EROFS group needed construction. The current
  preflight fast path requires every group to exist; a partial miss follows the
  Docker image-pull/materialization path.

## Measured: eight imports do not fully explain the misses

The inspected requests imported eight distinct cache tags. Repeated progress
lines can make the textual import-line count nine; that is not a ninth import.
Import tags were matched to their current manifest digests with bounded registry
HEAD requests, then to cache-export digests recorded by these benchmark builds.

Warm-003 imported caches from cold indices `000, 001, 004, 005, 007, 008, 009,
010`; its own variant's cold-003 cache was absent. However, warm-021 imported the
same set, **including cold-009, its exact application variant**, and still reran
the late source/compile steps. Increasing the import limit is therefore not a
demonstrated complete fix.

The OCI configuration's uncompressed layer IDs show which result chain was
used. Warm-003 and warm-021 share layers 0–11 with cold-000. Cold-003 and cold-009
instead have different layer IDs at positions 7–11. Warm-021 thus imported its
matching prior variant but produced the application result on another valid
cached dependency/native chain. The cache progress ordering does not establish
which import argument had priority.

**Inference:** concurrent cold work created multiple valid result chains for the
same dependency recipe. Merging caches can select a dependency result whose
downstream application result is not reused. The observed parent-chain selection
and late rebuild are proven; the exact internal BuildKit cache-record decision
requires solver-level tracing or a controlled replay with a single selected
cache. Do not attribute this request solely to the eight-import ceiling.

## Measured: rebuilt outputs have identical content but different timestamps

For cold-003 versus warm-003, and cold-009 versus warm-021, small OCI layers were
downloaded and compared on the gateway within explicit byte bounds:

| Layer | Tar entries | Entries with changed content hash | Entries with changed mtime |
| --- | ---: | ---: | ---: |
| Python source copy, layer 12 | 1,509 | 0 | 5 |
| Python compile/smoke output, layer 13 | 1,606 | 0 | 1,594 |

Each pair has identical path sets and identical compared mode, uid, gid, link
target, entry type and PAX headers. Its uncompressed layer IDs nevertheless
differ. This is evidence of timestamp variation, not changed Python file
contents, in the inspected late layers. The large dependency layer was not
downloaded for content comparison; no claim about all of its metadata is made.

The final EROFS payload digests in the inventory are identical for each matching
cold/warm Python pair. The converter uses `mkfs.erofs -T 0`, so timestamp changes
can produce distinct source identities while the normalized EROFS bytes remain
the same. The component lookup key includes the source layer IDs and parent
chain; identical eventual EROFS bytes cannot prevent the initial lookup miss.

The same effect appears in the multistage case: cold-005 versus warm-017 has
different uncompressed layer IDs at positions 6–9. The inspected bundled-app
copy (layer 6) and smoke-result copy (layer 9) each contain three entries, no
changed content hashes and two changed mtimes. The other two final layers were
not content-compared in this inspection.

## Measured and inferred: multistage warm-017 waited for real compile work

Warm-017 (`app-change-2`, builder `167927353`) took 29.649 s in build/push and
10.360 s in EROFS publication, including 6.961 s of Docker materialization. Its
log contains **both** an uncached, unnamed-stage TypeScript compile/test vertex
`#18` lasting 22.2 s **and** a later `compile`-stage vertex `#26` marked `CACHED`.

The concurrent TypeScript-tools request warm-016 ran the same variant on that
builder and records a 22.2 s compile/test vertex `#21`. The two vertices have
identical sequences of 51 timestamped output lines, verified by the SHA-256
`4148f97c9757416e77b8c69a13b8638d0113f1fddf00e2a9f4f38b1dc9dfe137`.
Their executions began less than one millisecond apart according to the build
records. This strongly supports shared concurrent BuildKit work: warm-017 waited
for the common stage and then reused its result. It does not support describing
the whole request as an instant, pre-existing compile-cache hit.

Warm-013/warm-014 provide a second instance on builder `167927354`, variant
`app-change-1`: the tools request's vertex `#21` and multistage request's vertex
`#18` both ran for 23.0 s. Their 51 timestamped output lines have identical hash
`9b0adaf941d16a25dd5193e08d1f58401847d12cc3fb38cd7315363703d50ef9`.
Warm-014 also reports a later compile vertex `CACHED`. Shared work is an
inference from matching logs, timings, inputs and placement; no internal solver
trace was captured.

## Follow-up experiments and optimization priorities

1. **Avoid full Docker materialization for partial EROFS hits.** These Python
   cases pay about 33.7 s of Docker pull/materialization for a missing 5.3 MB
   tail group. Selectively materializing missing groups is directly motivated
   by this measurement; lower-layer, whiteout and metadata correctness must be
   preserved.
2. **Test a coherent cache choice before raising import fanout.** Replay a
   matching context on a fresh store using only its previous cache, then compare
   with the current merged imports. Consider exact context/dependency affinity
   in addition to the current Dockerfile hint. Warm-021 shows that importing a
   matching cache somewhere in the set does not guarantee complete reuse.
3. **Test normalized fixture output timestamps separately.** The inspected late
   layers have identical contents and varying mtimes. A fixture-only experiment
   can quantify avoidable duplicate source identities. Do not generalize that
   all image timestamps can be discarded without changing user-visible
   semantics, or infer that the existing `SOURCE_DATE_EPOCH` environment variable
   normalized all exported layer metadata.
4. **Report actual executed vertices and cached materialization separately.**
   Keep per-request phase timings and both cached and executed vertex evidence.
   A cached vertex may require downloading/extracting its result; a multistage
   request may wait for concurrently computed work and subsequently print
   `CACHED`. Do not classify a request from one cache marker alone.

These are follow-up candidates. The running workload and production cache policy
were left unchanged during measurement.
