# Sharing installed dependencies across project bases

New flat-image project preparations can choose an existing qualified base by exact filesystem overlap. Previously, each missing project used the same generic fallback, even when a different project already contained matching installed dependencies. Same-project task preparation continues to add bounded task deltas over the retained project base.

The immutable SQLite accelerator indexes paths, file contents and filesystem metadata for large files. It shortlists four candidates, then computes complete delta costs, including small files, deletions and hardlink closure. A different base must save at least 16 MiB and 20% of changed file bytes. The source download and index are reused for preparation; the delta writer still reauthenticates the archive and every target file. Publication and original-source alias registration require the existing full filesystem qualification. No package caches are deleted or metadata semantics relaxed.

The [production canary measurements](dependency-sharing-20261001.json) cover two deliberately selected, previously oversized ScaleSWE sources:

| Source | File bytes versus generic fallback | File bytes with selected base | Compressed task delta | New EROFS bytes |
| --- | ---: | ---: | ---: | ---: |
| galois PR108 | 1,354 MB | 816 MB | 507 MB | 616 MB |
| eeweather PR10 | 1,242 MB | 832 MB | 395 MB | 556 MB |

Values use decimal MB. Both reused two existing EROFS components and passed full runtime-contract filesystem checks: 48,225 and 82,562 entries, zero unexpected differences. Runtime mounts and injected files retain the documented verifier exceptions. Subsequent normal source-alias creates took 1.45 and 1.36 seconds, with successful exec/file checks and no new import builds. These are individual warmed checks, not a load-test latency guarantee. Logical delta reductions are 40% and 33%; no compressed generic-fallback comparison was built. Existing anchor storage is excluded from incremental bytes.

The first index covers 488 qualified bases in 48,635,904 bytes. Its snapshot does not automatically gain subsequent bases. Rebuild a new snapshot from retained preparations as the candidate set grows; existing jobs retain their assigned base and source pin across resumes. A policy change does not rebase accepted or completed jobs. New selections have request, authenticated source metadata and selection journals; public metadata archives include preflight resolutions even before a build identity exists.

Recreate an accelerator using the installed package and repository scripts:

```sh
python scripts/shared_dependency_index.py --pool /path/to/project-campaign/seeds --output /path/to/new-anchors.sqlite
```

Pass `--dependency-index /path/to/new-anchors.sqlite` to `prepare_project_image_campaign.py` (seed phase only), `prepare_shared_task_pool.py`, or `prepare_shared_task_image.py`. Index candidates require full source scans, recorded source indexes, at most eight EROFS components and at most 2 GiB of EROFS data. The index is a local accelerator, not a portable recovery or readiness certificate. After volume loss, hydrate the saved public metadata, rebuild and qualify bases from pinned upstream inputs, then recreate the index with the new private references and local paths. Upstream blob availability remains a recovery dependency.

Production grew the registry from 3,500 to the approved 4,000 provider units online. Before expansion only about 78 GiB remained above the protected 500 GiB reserve, with roughly 400 project seeds and thousands of small task deltas pending. The mounted filesystem grew to 4,226,407,653,376 bytes. Two bounded preparation queues now prioritize shared project bases and small same-project deltas; each permits three preparers, at most two gateway CPU cores and 4 GiB of gateway memory. Their original growth-accounting baselines remain intact. Expensive full-source evaluation imports remain paused. This is preparation in progress, not complete task coverage or a claim that every corpus fits in 4 TB.

## Duplicate retirement

The subsequent prepared-catalog audit found two sources with multiple retained variants: ScaleSWE optimizely PR102 and the .NET 8 SDK. Two redundant experiment manifests were retired after checking immutable public source identity, runtime configuration, equivalent filesystems, current aliases, active leases/routes, preparation plans and the dependency index. The retained .NET variants already shared every OCI layer and EROFS component; the ScaleSWE variants also shared nearly all stored content. This was not a large recoverable allocation.

The two retired image IDs remain usable as aliases to retained equivalent preparations. Their former catalog entries are marked superseded, and the cleanup plan preserves their original records for recovery. A third candidate was restored after the final check found its digest in a frozen campaign input catalog; its original manifest, retention, image record and catalog entry are preserved. It shares existing blob storage. Fresh checks passed both retained source aliases before retirement and all three preserved image IDs afterward, with no new imports. The [cleanup receipt](duplicate-retirement-20261001.json) records the scope and validation. Both preparation queues continued running throughout. No physical garbage collection was performed, no registry service interruption occurred, and no reclaimed disk bytes are claimed. Shared components, source staging manifests and BuildKit caches were left intact.
