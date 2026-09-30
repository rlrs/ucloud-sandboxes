# Compact preparation of flat task images

Task-specific upstream images count as required bases. The acceptance target is every inventoried task base prepared, with only separately measured small steps left live. `plan_image_pool.py report --require-all-task-bases` enforces that distinction; all 94 generic bases alone cannot pass. At the completed September 30 pilot snapshot, coverage was **742/35,984 source references**, not 100%. BIRD/NeMo and the unavailable actual training selection remain outside this inventory.

The new preparer retains a verified shared filesystem plus a delta for flattened ScaleSWE images. It downloads and authenticates the original source as temporary input, compares all tar entries against a retained same-project anchor, and publishes only the anchor plus delta. It preserves source runtime configuration. BuildKit uses these OCI layers; the existing EROFS builder reuses the anchor's signed component. There is no second retained full copy of the target OCI layer in this preparation path.

This is an offline preparation tool, not an automatic rewrite in the request path. After qualification it inserts ordinary upstream-name and digest aliases. Existing different aliases are preserved. A production check created a new task image through its original public name in 1.146 seconds without an import build; no SDK update is required. That single check is not a load-test latency guarantee.

## Measured pilot

[Evidence](benchmarks/image-pools-2026-09-30/shared-task-preparation.json) records 48/48 successful preparations over 12 project anchors. Their compressed deltas total 59,254,673 bytes; their original compressed layers total 26,474,609,014 bytes. Observed total registry growth was 159,645,696 bytes. This excludes already-retained anchor costs and includes concurrent registry activity. Some original images already share EROFS blobs, so avoiding their full OCI layers is a separate saving from filesystem-component sharing. No original images were deleted.

There were 36 full source filesystem scans and 12 checks using a prior exact-filesystem certificate plus a fresh sandbox mount/exec. One initial verification failure involved Helm files under `/tmp`; the deployed sandbox always replaces `/tmp` with tmpfs. The verifier now follows that runtime contract, and the image passed on retry. Failed verification did not register its source aliases.

The expanded queue contains 8,565 candidates over 118 retained anchors. It runs two children at a time, under 150% gateway CPU and 3 GiB memory limits, with a **persisted 16 GiB growth allowance** and **500 GiB free-space floor** on the existing 3,000 provider-GB volume. A non-overlapping continuation can use up to 64 GiB total growth from that same baseline after the initial stage exits. Storage-deferred rows are reconsidered against current admission on resume; the floor is unchanged. Candidates are not coverage. Storage admission includes a 2 GiB reservation per active child. New projects can have much larger deltas than the initial pilot; actual growth determines how far the batch proceeds.

## Qualification and failure behavior

- Authenticate compressed source/config digests, signed prepared artifacts, source-layer bindings and target runtime configuration before aliases become usable.
- Reject unsupported tar entries, unsafe paths, invalid hardlinks, implicit ancestors, multi-layer sources, non-amd64/Linux sources and `ONBUILD` recipes. The current path is deliberately limited to flat sources. The other inspected SWE families contain multi-layer images and continue using their existing preparation path.
- Bound compressed inputs to 2 GiB each, indexes to 200,000 entries/64 GiB regular-file bytes, and changed regular-file data to 256 MiB. Large changes are deferred.
- Compare actual sandbox contents, metadata, links and xattrs against the authenticated source. Follow existing EROFS timestamp normalization (`mkfs -T 0`) and runtime mounts/injected files; this does not promise preservation of Docker's original timestamps or files hidden by sandbox mounts.
- Reuse a full scan for at most 24 hours only when expected source contents, physical component identities/order/ranges/formats, runtime config, scanner contract and worker bundle match. Every image still gets a fresh sandbox mount/exec. Source provenance remains authenticated separately.
- Preserve accepted and successful build receipts. A successful artifact can be rechecked after its builder scales down and job history disappears. Cleanup removes only this locked job's marked, recognized temporary input files.
- Respect the shared public-registry cooldown outside child admission. Source failures, unsupported cases and storage deferrals never count ready. Storage deferrals are reconsidered on resume without resetting the accounting baseline. `--retry-failed` archives prior failed journals and preserves the original budget baseline.

The scanner requires Python 3 in the source image. Qualification establishes the platform filesystem contract, not execution of every benchmark test or grading command. Full upstream downloads and indexing remain offline costs; the accepted backend build duration alone understates preparation wall time.

## Run and resume

Use a compatible package containing `oci_flat_delta` and `flat_image_qualification`, the repository scripts together, and the deployment service account. The production preparer uses an isolated package directory; the gateway, worker runtime and SDK were not replaced. The qualified wheel SHA-256 is `83ace15dcc74c487967c8b5f6a2317db85f7874fd069c71961c791c6b32e0cb0`.

```sh
python scripts/plan_shared_task_pool.py \
  --inventory /data/source-inventory.json \
  --catalog /data/original-source-catalog.json \
  --output /data/shared-tasks/plan.json

python scripts/prepare_shared_task_pool.py \
  --root /data/shared-tasks --gateway https://sandbox.example \
  --sdk-wheel /data/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl \
  --workers 2 --limit 8565 --growth-limit-gib 16 --free-floor-gib 500
```

Run under a durable supervisor with the resource limits above. Resume with the same root and budget file; do not reset the baseline to make deferred work admissible. `results/` contains per-source outcomes, `catalog.json` and `progress.json` checkpoint every four completions, and each child retains its source resolution before building. Plans include public anchor provenance as well as currently prepared private references. An inventory `pinned_source` is passed through and enforced during recovery.

## Recovery

The [portable inputs](../image-campaigns/2026-09-30/README.md) preserve 743 public source pins and all 8,602 foundation contexts independently of the registry volume. `shared-anchors.json` contains the 118 original public anchor inputs without private references or completed-build state.

After volume loss, use a fresh recovery generation. Restore the required anchors from that public plan with the normal source preparer, adding a fresh `rebuild_generation` to its plan. Materialize the portable bundle for pinned source inventory, then regenerate the shared-task plan from the **new** anchor catalog. This derives new private references and preparation identities. Never reuse stale successful catalogs, old private anchors or a previous shared-task work directory after losing their registry contents. Existing aliases are insert-only; use explicit restored references and rewrite/audit the task index as described in the campaign guide.

Refresh the off-host inputs as more images complete:

```sh
python scripts/image_campaign.py refresh \
  --bundle image-campaigns/2026-09-30/inputs-shared-pilot.json.gz \
  --catalog /data/shared-tasks/catalog.json \
  --output /data/inputs-next.json.gz
```

Refresh preserves every earlier pin and foundation byte, rejects conflicting source pins and writes no success receipts or credentials. Preserve the resulting bundle off-host. Public images can disappear; recipe recovery is not a registry-blob backup or a guarantee of a byte-identical rebuild.

A larger expanded-queue delta was profiled separately: 253,924,323 changed logical bytes were new files, 1,933,116 were timestamp-only changes and 332,650 were changed file contents. Its large size is therefore not explained by redundant timestamp copying. These are logical bytes, not compressed savings.
