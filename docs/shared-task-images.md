# Compact preparation of task images

Task-specific upstream images count as required bases. The acceptance target is every inventoried task base prepared, with only separately measured small steps left live. `plan_image_pool.py report --require-all-task-bases` enforces that distinction; all 94 generic bases alone cannot pass. At the completed September 30 pilot snapshot, coverage was **742/35,984 source references**, not 100%. BIRD/NeMo and the unavailable actual training selection remain outside this inventory.

The new preparer retains a verified shared filesystem plus a delta for flattened ScaleSWE images. It downloads and authenticates the original source as temporary input, compares all tar entries against a retained same-project anchor, and publishes only the anchor plus delta. It preserves source runtime configuration. BuildKit uses these OCI layers; the existing EROFS builder reuses the anchor's signed component. There is no second retained full copy of the target OCI layer in this preparation path.

This is an offline preparation tool, not an automatic rewrite in the request path. After qualification it inserts ordinary upstream-name and digest aliases. Existing different aliases are preserved. A production check created a new task image through its original public name in 1.146 seconds without an import build; no SDK update is required. That single check is not a load-test latency guarantee.

## Measured pilot

[Evidence](benchmarks/image-pools-2026-09-30/shared-task-preparation.json) records 48/48 successful preparations over 12 project anchors. Their compressed deltas total 59,254,673 bytes; their original compressed layers total 26,474,609,014 bytes. Observed total registry growth was 159,645,696 bytes. This excludes already-retained anchor costs and includes concurrent registry activity. Some original images already share EROFS blobs, so avoiding their full OCI layers is a separate saving from filesystem-component sharing. No original images were deleted.

There were 36 full source filesystem scans and 12 checks using a prior exact-filesystem certificate plus a fresh sandbox mount/exec. One initial verification failure involved Helm files under `/tmp`; the deployed sandbox always replaces `/tmp` with tmpfs. The verifier now follows that runtime contract, and the image passed on retry. Failed verification did not register its source aliases.

The expanded queue contains 8,565 candidates over 118 retained anchors. The current coordinator runs four children under 300% gateway CPU and 6 GiB memory limits, with a **persisted 64 GiB growth allowance** from the original baseline and **500 GiB free-space floor**. The earlier two-worker coordinator and waiting continuation were drained/stopped before this replacement started. The registry was subsequently expanded to 3,500 provider GB for additional evaluation coverage; the original accounting baseline was preserved. Storage-deferred rows are reconsidered against current admission on resume; the floor is unchanged. Candidates are not coverage. Storage admission includes a 2 GiB reservation per active child. New projects can have much larger deltas than the initial pilot; actual growth determines how far the batch proceeds.

## Qualification and failure behavior

- Authenticate compressed source/config digests, signed prepared artifacts, source-layer bindings and target runtime configuration before aliases become usable.
- Reject unsupported tar entries, unsafe paths, invalid hardlinks, implicit ancestors, non-amd64/Linux sources and `ONBUILD` recipes. Flat sources use the existing direct path. Multi-layer sources require protected offline exports and the additional qualification described below.
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

## Qualified multi-layer preparation (October 1)

The [measured results](benchmarks/image-pools-2026-09-30/layered-task-preparation-20261001.json) include 14 ordinary imports across seven families, an existing-image sharing proof, and two fresh R2E-Gym preparations. The fresh sources required **37.7 MB and 42.1 MB** compressed deltas, plus **68.4 MB and 77.9 MB** of new EROFS data; each reused seven existing components. Their original compressed sources were about 448.6 MB each. Existing anchor costs are excluded. The earlier 507.4 MB source / 168.2 MB delta proof used an already imported target and did **not** reclaim its original blobs.

The existing-target proof and three fresh layered preparations passed independent full sandbox filesystem scans. The third fresh source also exercised the final disk-bounded unpacker: a 171.4 MB delta replaced a 510.6 MB source, and its temporary filesystem was removed. Fresh-source requests through the ordinary public names created sandboxes in 1.34 and 1.47 seconds without an import build. These are small canaries, not a workload latency guarantee. The full-source checker now accounts explicitly for the runtime-created `/workspace` directory only when absent from the source; existing workspace contents, ownership, mode and timestamps remain checked. This advances the certificate contract to version 3 and forces old certificates to be requalified when using the new package.

`export_oci_filesystem.py` verifies the pinned source manifest/config and compressed blobs, then invokes a SHA-256-pinned distro `umoci` binary inside a minimal chroot with no network, no image execution, and a read-only root. Unpacking writes to a dedicated, temporary **16 GiB ext4 filesystem**, mounted `nodev,nosuid,noexec`; decompression cannot consume unbounded gateway root space. Source inputs are at most 5 GiB compressed / 64 layers. Export preserves numeric ownership, modes, links, timestamps and xattrs, following the existing EROFS excluded runtime trees. It removes the unpacked filesystem and source-blob scratch after a successful export. A failed unmount preserves its backing file for investigation instead of deleting a mounted filesystem.

Example workflow, using new directories and immutable references:

```sh
# Run exports as root. Use an authenticated resolved.json for the public target.
python scripts/export_oci_filesystem.py --reference "$ANCHOR_PIN" \
  --root /data/exports/anchor --umoci /opt/umoci --umoci-sha256 "$UMOCI_SHA256"
python scripts/export_oci_filesystem.py --reference "$SOURCE_PIN" \
  --resolved /data/source-receipt.json --root /data/exports/target \
  --umoci /opt/umoci --umoci-sha256 "$UMOCI_SHA256"
```

Keep the export parent, child directories, archives and reports owned by root and not group/other writable; grant the service account read/traverse access. Run `prepare_shared_task_image.py` as that account with its usual arguments and `--filesystem-exports /data/exports`. Exports are bound to the authenticated source reference, config, ordered layer descriptors and diff-ID chain. The preparation identity also includes both export digests. Only the new delta is uploaded; all anchor blobs are mounted from the retained registry. Full filesystem qualification and persistent retention precede insert-only public alias registration.

The deployed experimental package is isolated from the running flat queue and runtime services. Its distro unpacker is Ubuntu `umoci 0.4.7+ds-4`, binary SHA-256 `4796f71e1dc93959b5d5c33066ee5a51542a16879df92a21fc0d6227a62d89d6`; the package was extracted, not installed. The default changed-file bound remains 256 MiB; the individually reviewed layered proofs used 512 MiB. This path is qualified for these cases, not yet an unattended whole-corpus layered coordinator. Export archives also need a bounded retention policy before large-scale reuse.

## Storage expansion and upstream limits

The volume increased from 3,000 to **3,500 provider GB**, within the user's conditional 4,000 GB ceiling. Ninety pinned evaluation candidates were missing 42.134 GB of distinct OCI blobs. The 14-image batch added 3.069 GB of OCI data and 4.989 GB of EROFS, a 1.63 ratio; extrapolating that selected sample gives roughly 111 GB of additional data, beyond the approximately 65 GB then available above the 500 GiB reserve. This justified additional capacity, but does not establish a whole-corpus storage forecast. No automatic resize is configured.

Confirmed Docker Hub HTTP 429 responses now pause unresolved work through the shared cooldown. The source resolver records HTTP status codes so throttling is distinguishable from 502/503/504 failures. Ninety pinned evaluation bases and 24 pinned training bases can build without another manifest lookup. The remaining 643 evaluation candidates are queued for actual preparation under that cooldown. They are not counted as ready. Large unsupported inputs remain explicit deferrals.

The new metadata inventory reports the distinct missing OCI union as a lower bound and writes source receipts before continuing. It is not an EROFS capacity estimate or a readiness catalog. Metadata-only probing was stopped once its saved receipts were handed to preparation; this avoids competing discovery and preparation requests for the same sources.

The two pinned source batches were resumed at four workers each using their original journals, accepted build IDs and storage baselines. Each coordinator retains a 100% gateway CPU cap and 2 GiB memory limit. The provider builder ceiling stays at eight; autoscaling determines actual node count. Five additional pinned R2E sources use the bounded compact path serially under a separate 100% CPU / 3 GiB cap. These queued sources remain outside ready coverage until qualification succeeds.
