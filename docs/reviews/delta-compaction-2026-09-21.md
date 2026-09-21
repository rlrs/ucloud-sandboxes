# Delta-only snapshot compaction

Implemented after release 0.5.69; not deployed. Both Registry and S3 use the
same selection policy. No native binary, publication schema, SDK, admission
limit or configured threshold changes are required.

## Change

The existing eight-layer depth trigger previously flattened the entire chain,
including a large mostly unchanged base. When depth alone triggers maintenance,
a published base larger than all newer layers combined is now retained verbatim.
Only those newer layers are exported and uploaded as one merged delta. The new
manifest references the original base followed by that merged delta, reducing
the chain to two layers while avoiding at least half the estimated input bytes.

A one-layer threshold still forces a full merge. So do accumulated deltas above
the byte threshold, a base that no longer dominates the chain size at a depth
trigger, and any change of blob origin. Layers that have not yet been published
are not treated as reusable bases. The byte trigger and layer-depth bound remain
in force; this adds no client rejection or worker admission cap.

Only the selected suffix appears in the native compaction input. Existing blob
origin handling remains intact for those remote inputs. Upload accounting counts
only the newly exported layer, and Registry compaction input/output metrics
exclude the retained base. Both publishers annotate the compaction span with
retained-base bytes. Prior descriptors and local deltas remain valid until the
new metadata commit succeeds; no publication fencing or garbage-collection
ownership rules change.

Keeping the base can retain some overwritten remote data longer than a full
merge would. Growth past the existing byte budget forces a full merge, and
normal chain reads still resolve newest data first. We deliberately keep the
existing streaming exporter and bounded cache rather than creating a second
full temporary image on the worker.

## Verification and measurement

The canonical repository check passed 1,002 server tests (six platform skips),
118 SDK tests, Ruff, shell syntax, Go tests, wheel builds and isolated installs.
Shellcheck is unavailable locally and was explicitly skipped using the supported
check-script override. Targeted publication tests cover both backends through
real Unix-socket streams with test object stores: base-descriptor retention,
base exclusion from compaction input, correct uploaded-byte accounting,
accumulated-byte and origin-switch fallbacks, repeated bounded-depth merges, and
preservation of the previous publication after an invalid export digest.

The standalone native qualifier ran against the already pinned storage binary
on idle UCloud qualification worker `12397867`. It builds temporary sealed
OverlayBD layers and uses export RPCs only: no device creation, mounts, live
journal changes or provider stop requests. It verifies final logical block data
independently and uses the native exporter again to restack the base plus merged
delta. Explicit zero writes, discard over base data, overwrite after discard,
repeated overwrite and previously unmapped holes all passed.

A 128-MiB base plus eight small deltas, with three alternating full/delta runs:

| Native export | Median | Output bytes |
| --- | ---: | ---: |
| Full chain | 308.98 ms | 134,225,920 |
| Deltas only | 3.19 ms | 24,576 |

This fixture reduced streamed bytes by 99.98% and local export time by about
97 times. It is a small-delta, warm-local-file measurement; it excludes remote
fetch, upload latency and concurrent application load. It does not establish
that the previous production 7.2-second publication tail falls by the same
factor. Larger or heavily overwritten delta chains may select a full merge.

The native test used binary SHA-256
`75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`.
Raw results: [delta compaction benchmark](../benchmarks/delta-compaction-2026-09-21.json).
Reproduce with `runtime/storage_native/benchmark_delta_compaction.py`; its README
documents the invocation. Two initial harness attempts failed before validation
because of a remote quoting error and an output filename collision; both were
corrected before the successful complete run.

## Production authentication encountered during qualification

The local UCloud refresh session expired, and the production autoscaler also
entered a restart loop with 403 errors from the jobs API. After the user signed
in again in Firefox, the supported CLI import renewed the local session. The
same renewed session was installed atomically with mode 0600 in the existing
production credential file, and only the autoscaler service was restarted.
Provisioning resumed and created the qualification worker. The temporary local
credential-transfer script was removed. Qualification reservations were removed
after each attempt; the normal autoscaler retains ownership of worker cleanup.
The production runtime remains 0.5.69.
