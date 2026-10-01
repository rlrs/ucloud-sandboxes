# Base-first expansion deployed October 1

Production unit `ucloud-base-expansion-20261001` is running from `/work/ucloud-sandboxes/base-expansion-live-20261001`. The previous two preparation queues were drained before launch. Gateway, registry and sandbox services were not restarted. A short SSH agent interruption was resolved; deployment used a persistent connection.

The refreshed 12:51 CEST catalog contains 3,595 ready source images and qualified examples for 985 ScaleSWE project groups. The source counts remain distinct from cheap-tail qualification and dataset split coverage. Free space before launch was 989 GiB, approximately 489 GiB above the reserve.

## Work admitted

Metadata preflight completed at 12:59 CEST. Of 106 selected representatives, 102 have authenticated, pinned metadata and satisfy the 2 GiB compressed-source admission bound:

- 29 SWE-Lego projects
- 32 SWE-rebench projects
- 31 SWE-smith environments, preserving exact enriched preparation
- Eight SWE-bench Verified projects
- Two SWE-bench Multilingual projects

Two sources (pywatts and keras-nlp) exceeded the size bound; dimod and voluptuous were deferred by Docker Hub cooldowns. The admitted list is a preparation queue, not completed coverage. All admitted sources have multiple OCI layers, so they use normal layer-preserving preparation. Existing blob mounts and EROFS layer reuse remain enabled. By 13:04 CEST, the Vue Multilingual base had passed publication and runtime checks, SymPy was building, and one source was recorded ready with no recorded preparation failures. Existing-layer mounts were observed. The remaining queue is unfinished. Gateway `/healthz` and registry health both returned HTTP 200; free space was 985 GiB.

The [admitted source plan](base-expansion-admitted-20261001.json) contains resolved pins. [Authenticated metadata](../../../image-campaigns/2026-10-01/base-expansion-source-receipts.json.gz) is saved off-server: 104 valid receipts, including the two oversized sources; SHA256 `084b0e3c750635a4c3b93ef79c44f1043fac5938d1049905019e53f3e62bd2e1`. Earlier portable bundles retain the full foundation contexts. The live campaign preserves per-source journals and input hashes.

## Execution and storage

`run_base_expansion.py` serializes stages with two workers, `CPUQuota=200%`, `CPUWeight=10`, `Nice=19` and `MemoryMax=4G`. All new stages share the persistent initial used-byte measurement `2971153498112`; resuming cannot reset the allowance.

1. New source representatives: cumulative growth limit 64 GiB, with no duplicate allowance for the normal/shared routes.
2. 32 Terminal-Lego prefixes: cumulative limit 96 GiB.
3. 32 TMax prefixes: cumulative limit 128 GiB.
4. Resume the existing ScaleSWE project campaign with `--seeds-only`: cumulative limit 192 GiB. The old campaign baseline is preserved and the allowance translated conservatively into that baseline.

The 500 GiB free-space reserve applies at every stage; volume size remains 4,000 provider units. Allowances bound admission rather than predicting final storage. The expensive per-task completion queue remains paused. Individual oversized, failed or unavailable inputs do not become coverage and can be addressed later. A failed coordinator requires inspection/resume; this deployment does not install an automatic reboot recovery policy or promise that every selected case will fit.

One alternate prefix catalog conflicted with other receipts for the same key. The launcher chose a consistent existing catalog set covering 2,277 prefix keys, preserving all stored variants. It did not delete data.

Validation: 69 tests cover selection, baseline preservation, cumulative allowance translation, project campaigns and underlying preparation; lint passed. Production registry health returned HTTP 200 after the source stage began. Metadata and source-stage progress are recorded separately from successful preparation.

For future snapshots, include the nested `campaign/stages/*` and prefix directories as catalog roots; the earlier top-level-only snapshot helper does not discover this layout automatically.
