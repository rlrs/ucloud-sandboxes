# Delta-only snapshot compaction

Deployed as release 0.5.70 on September 21, 2026. Both Registry and S3 use the
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
The production runtime was 0.5.69 during that native qualification; the
subsequent deployment is recorded below.

## Release 0.5.70 deployment

Runtime commit `b285051b2331a58794801b721e68eeac9256fb36` was committed and
pushed. Gateway deployment completed at 12:17:18 UTC; all 93 installed package
files matched the wheel. Both sandbox and builder bundles passed Linux boot
validation, and future nodes use the 0.5.70 bundle directory. The 62 targeted
Linux tests and both Python 3.10/3.13 [CI jobs](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35598489561)
passed. Native storage and gVisor binaries are unchanged.

The previous idle qualification worker scaled down normally before the worker
rollout reached it. Fresh worker `12397877` then booted on 0.5.70 from the new
bundle. The old quarantined worker was left untouched. The deployment procedure
issued no provider stop requests. No SDK or Verifiers change is required.

An initial eight-cycle smoke passed but produced one through eight layers,
without triggering compaction. After correcting a property-access error in the
post-run manifest checker, a fresh 16-cycle smoke exercised two depth triggers.
Every park, publish/detach and SDK-exec wake passed, preserving state and process
identity, with zero lifecycle retries. Manifest layer counts were
`1,2,3,4,5,6,7,8,2,3,4,5,6,7,8,2`. All sixteen manifests retained the same
239,546,368-byte base. The merged delta at the second compaction was 8,892,416
bytes.

Publish/detach took 0.515 s on compaction cycle 9 and 0.562 s on cycle 16. Across
all cycles, median park was 0.222 s, publish/detach 0.452 s, and SDK-exec wake
0.913 s. Maximum publish/detach was 2.228 s (initial publication); maximum wake
was 0.973 s. This was a single-sandbox smoke on an idle worker, not a loaded
throughput qualification or a controlled comparison against the prior release.

Final public health checks passed 20/20, median 19.9 ms and maximum 47.1 ms.
Gateway, relay and autoscaler were active; the worker was fresh, admission open
and unquarantined. All smoke sandboxes and reservations were removed, and there
were no remaining routes, pending creates or capacity reservations. The normal
autoscaler owns idle-worker cleanup. Worker publication counters (24 publications,
two compactions) include both the eight-cycle and sixteen-cycle smokes.

Raw release evidence: [16-cycle smoke](../benchmarks/release-0.5.70-live-smoke-2026-09-21.json).
