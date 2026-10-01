# Expand reusable bases before completing task images

The user authorized this priority change on October 1. This replaces the earlier proposal to qualify 58 R2E/MultiSWE task differences or finish all ten remaining Multilingual task images first. Neither new production preparation nor production queue reprioritization has happened: the forwarded SSH agent is absent and the gateway rejects the available local key.

The offline planner now selects one representative for each missing project, keeps distinct SWE-smith dependency environments separate, and selects missing dependency prefixes. It orders each family by potential task fanout and interleaves families. Existing qualified project examples suppress additional task variants, including examples above the earlier 2 GiB reporting bound; larger examples still need cost qualification. Input pin mismatches, unqualified derived images, and incorrect preparation recipes do not count as existing bases. Active assignments can be excluded without counting them as prepared.

The first tranche, using the saved October 1 09:27 CEST image receipts and 08:28 CEST prefix receipts:

| Family | New representatives | Rows in represented groups |
| --- | ---: | ---: |
| SWE-Lego | 32 | 273 |
| SWE-rebench v2 | 32 | 369 |
| SWE-smith | 32 | 7,395 |
| SWE-bench Verified | 8 | 212 |
| SWE-bench Multilingual | 2 | 10 |
| Terminal-Lego dependency prefixes | 32 | 47 |
| TMax dependency prefixes | 32 | 46 |

The project figures are potential reuse, not newly prepared task counts or certified inexpensive deltas. SWE-smith's rows require the exact enriched preparation. The 106 source representatives include 98 without saved immutable pins and 98 without matching size metadata. They need authenticated metadata resolution and the 2 GiB compressed-input admission check before building; unknown sizes are not assumed small. Eight have saved size metadata, but none currently qualifies for the single-layer sharing path. The planner can route known flat sources through the existing dependency-index preparer as metadata becomes available. Multilayer sources use normal layer-preserving preparation unless separately qualified for filesystem sharing; never force them through the flat-image path.

R2E-Gym and MultiSWE need no additional project representatives under this accounting. OpenSWE's 11 planned foundations are already retained; its remaining 495 recipes need additional recipe analysis, not duplicates of those foundations. Generic bases are already present for TBLite, Senior, Terminal-Lego and Terminal-Bench 2. BIRD and NeMo definitions remain unavailable. Heldout split membership is still unverified.

There are many more missing projects than this first tranche: 1,434 SWE-Lego and 1,827 SWE-rebench groups, plus 346 currently admissible SWE-smith environment candidates. The stricter full-scan proof requirement excludes one ScaleSWE project from base reuse, explaining the difference from the earlier general prepared-image count. Original bases with many layers remain eligible. Sources missing metadata may still be deferred after resolution. MONAI is already over the admission bound. Re-run the planner against refreshed receipts after each tranche; it skips completed groups instead of progressing automatically to their task variants.

## Production cutover when access returns

1. Refresh catalogs, source receipts, prefix receipts, current free space and active queue assignments. Re-plan with each active plan supplied as `--exclude-plan`; queued work is not coverage. Recheck retained artifact availability before relying on it.
2. Let active preparations finish and reprioritize the existing per-task queue. Resume the existing ScaleSWE project campaign with `--seeds-only`, preserving its original storage baseline and job journals. That option prevents the automatic task phase; it does not stop a different service that is already running.
3. Keep the 4,000-provider-unit volume ceiling and 500 GiB free-space reserve. Reallocate existing allowances instead of adding overlapping budgets. Initial additional publication allowances: 64 GiB for source representatives, 32 GiB for Terminal prefixes, 32 GiB for TMax prefixes. These are admission limits, not storage forecasts; unknown and decompressed sizes may cause deferrals. Run at most one tranche coordinator at a time, with two preparation workers, until fresh load measurements justify more.
4. Hydrate matching saved source resolutions with `cache_source_receipts.py`, resolve missing metadata, then re-plan so eligible flat sources use dependency sharing. Use `prepare_shared_task_pool.py --record-source-index --dependency-index ... --max-delta-mib 1024` for that routed plan, and `prepare_image_pool.py --stage-upstream --max-image-gib 2` for normal source plans. Preserve exact SWE-smith enrichment. Share the source allowance across these routes; do not give each a fresh 64 GiB budget.
5. Use `prepare_image_foundations.py` for the generated prefix contexts, with the refreshed base catalogs and existing prepared-prefix roots. Do not rebuild generic bases. Keep independent source-equivalence/context checks and publication receipts; candidate selection alone never makes an image ready.

No new service has been launched and no existing service has been stopped by this change.

## Reproduce the first tranche

```sh
PYTHONPATH=scripts .venv/bin/python scripts/plan_base_expansion.py \
  --inventory build/cache-expansion-20261001/inventory.json \
  --catalog build/cache-expansion-20261001/aggregate-catalog-20261001T072720Z.json \
  --source-receipts build/cache-expansion-20261001/source-receipts-20261001T072720Z.json.gz \
  --foundation-bundle image-campaigns/2026-09-30/inputs-projects-20261001.json.gz \
  --foundation-snapshot build/cache-expansion-20261001/foundation-capacity-20261001.json \
  --fallback-anchor-source aweaiteam/scaleswe:pallets_click_pr1000 \
  --output /tmp/new-base-expansion
```

The output contains normal/shared source plans and validated prefix build contexts. Existing output directories cannot be overwritten. Source pins, size gaps, fanout and required preflight are recorded in [the selected source plan](base-expansion-sources-20261001.json). [The expansion receipt](base-expansion-20261001.json) records input hashes and selected prefix keys. Original inputs and recipe contexts remain in the existing recovery bundle; package repositories remain external dependencies.

Validation: 66 tests across base selection, project campaigns, readiness accounting, shared preparation and image pools; lint passed. All 64 selected prefix contexts also passed their content-hash validation during materialization.
