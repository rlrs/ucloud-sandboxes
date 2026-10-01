# Portfolio preparation accounting and next batch

Image receipts: October 1, 2026, 09:27 CEST. Foundation receipts are the saved 08:28 CEST October 1 measurement, matched by exact keys against the portable input bundle. A fresh production read was unavailable because the forwarded SSH agent disconnected. These are upstream pool counts, not a verified training or heldout selection. The [machine-readable accounting](portfolio-readiness-20261001.json) records input hashes and separates each coverage level.

“Prepared” means the task's upstream image or exact enriched preparation is retained in the catalog. It does not certify arbitrary agent setup or grading steps. “Base candidate” additionally counts pending tasks with an eligible prepared example from the same project within the same environment family. It requires a faithful original or a fully source-scanned flat delta, at most eight components and at most 2 GiB of EROFS data. Candidate differences have not all been measured. Larger bases and cross-environment matches may provide additional reuse, but are excluded from this bounded count. Small metadata differences do not qualify an arbitrary RUN/COPY step as cheap.

| Training environment | Prepared task rows | Prepared + same-project base candidate | Other cached preparation / remaining work |
| --- | ---: | ---: | --- |
| ScaleSWE | 1,699 / 17,202 (9.9%) | 15,806 / 17,202 (91.9%) | Continue cheap task deltas; 1,396 lack a bounded same-project candidate |
| R2E-Gym | 67 / 4,522 (1.5%) | 4,522 / 4,522 (100%) | Prepared examples in all 10 projects; cost qualified only for sampled images, including the aiohttp pilot |
| MultiSWE | 60 / 2,232 (2.7%) | 2,187 / 2,232 (98.0%) | Prepared examples in all 43 projects; 45 pending tasks have only larger bases under this accounting |
| SWE-Lego | 62 / 4,323 (1.4%) | 1,198 / 4,323 (27.7%) | 62 of 1,496 projects have prepared examples; adding bases matters here |
| SWE-rebench v2 | 63 / 6,272 (1.0%) | 1,254 / 6,272 (20.0%) | 63 of 1,890 projects have prepared examples |
| SWE-smith | 59,879 / 88,130 (67.9%) | 59,879 / 88,130 (67.9%) | 103 of 450 source environments; remaining 347 represent 28,251 rows. Exact enriched recipe required |
| OpenSWE | Complete task-recipe readiness not established | Not counted as project-base candidates | 11 cached Python foundations match 36,389 / 36,884 recipes (98.7%); project dependency installation remains |
| Terminal-Lego | Complete task-recipe readiness not established | Not counted as project-base candidates | Generic bases cover 13,825 / 13,825; richer cached prefixes cover 9,000 / 13,825 (65.1%) |
| TMax | Complete task-recipe readiness not established | Not counted as project-base candidates | Explicit and inline dependency prefixes cover 12,956 / 14,600 recipes (88.7%); task-specific work remains |
| BIRD SQL / dev SQL | Unknown | Unknown | Exact definitions were deferred; no readiness credit |
| NeMo Calendar / heldout | Unknown | Unknown | Definitions deferred |
| NeMo Instruction / heldout | Unknown | Unknown | Definitions deferred |
| NeMo Pivot / heldout | Unknown | Unknown | Definitions deferred |
| NeMo Workplace / heldout | Unknown | Unknown | Definitions deferred |

TMax's count combines 5,134 recipes using the explicit shared installer and 7,822 with a prepared inline prefix. These populations are disjoint: inline planning excludes contexts containing an explicit base installer. The remaining 1,644 includes uncached prefixes and unsupported layouts. Terminal's full denominator is 13,825, not just the 12,545 recipes admitted to prefix planning. Summing duplicate foundation catalogs or shared generic-base fanouts would inflate these figures; the report avoids both.

| Evaluation environment | Prepared image/base state | What is still needed |
| --- | --- | --- |
| SWE-bench Verified | 274 / 500 exact task sources; 4 / 12 project groups | Eight missing project groups contain 212 tasks; another 14 pending tasks only have larger same-project examples |
| SWE-bench Multilingual | 290 / 300 exact task sources | Remaining 10 are five uutils/coreutils and five vuejs/core images, about 1.09–1.17 GB of upstream compressed layers each; final incremental storage is unmeasured |
| Terminal-Bench 2 | 89 / 89 task source images | Agent/verifier setup still needs episode-level qualification |
| OpenThoughts TBLite | All 16 generic bases serving 100 task recipes | Full task recipes and verifier setup not bulk-qualified |
| Senior SWE-Bench | All 13 generic bases serving 50 task recipes | Full task recipes and common verifier/validation dependencies not bulk-qualified |
| BrowseComp-Plus | No per-question image in the pinned adapter | Shared BM25 corpus/index/tokenizer service; service readiness is unverified |

R2E-Gym, ScaleSWE, SWE-Lego and NeMo heldout variants do not get separate percentages without actual split membership. The BIRD dev view is similarly unknown. Bash/OpenCode/Pi variants are not additional task filesystems; their selected toolkit setup needs separate qualification. Judge model selection does not by itself require another copy of an image.

## Recommended next preparation allocation

Keep the existing ScaleSWE small-delta queue. Give the following staged work priority over expanding low-fanout ScaleSWE project seeds. These are proposed admission caps, not predicted storage costs or submitted production jobs. Recheck live queues, resolve every missing source pin and reallocate their existing growth allowances before starting. Keep the 4,000-provider-unit ceiling and 500 GiB reserve. The last confirmed headroom above that reserve was approximately 542 GiB.

1. **R2E-Gym and MultiSWE: 58 project-diverse delta cases, 32 GiB cap.** Two pending images in each R2E project and one in each eligible MultiSWE project that still has a pending task: 20 + 38 cases. Measure and fully qualify differences over retained bases, then prioritize additional tasks in the families with cheap observed deltas. This addresses large gaps without buying a new base for every task.
2. **SWE-smith: 20 missing environments, 32 GiB cap.** These represent 4,855 additional task rows if their exact enriched recipes pass. Known oversized sources, including MONAI and FVCore, are deferred. Sources without saved size metadata undergo metadata-only preflight and a 2 GiB compressed-input bound before any build. This is an admission rule, not proof of decompressed size or total incremental storage.
3. **SWE-Lego and SWE-rebench: ten missing project examples each, 32 GiB cap.** They cover project groups containing 222 task images. Only the 20 prepared examples become immediately ready; the rest still need delta qualification. Compare against existing dependency bases, including suitable cross-family candidates, before retaining new independent bases.
4. **Evaluation: ten Multilingual images plus eight missing Verified project examples, 16 GiB cap.** Finishing the ten completes that inventoried Multilingual image pool. The eight Verified examples could provide bases for the remaining 212 tasks across those projects; they do not complete all 212.
5. **Measure the remaining recipe steps, 16 GiB cap.** Six representative recipes each from OpenSWE, TMax and Terminal-Lego; five each from Senior and TBLite. These 28 cases are a recipe-level qualification target, not yet an exact submitted recipe list. Promote only cases with a complete preparation or measured bounded remaining work. Prefer package-environment preparation over adding more generic Python/Ubuntu bases.

Total proposed additional publication allowance: **128 GiB**, spent in stages with measured outcomes before expansion. It is deliberately smaller than available headroom, and must share the volume budget with ongoing preparation. It cannot guarantee all selected sources fit. Maintain intended environment weights and original dataset splits when creating the eligible training cohort; uniform sampling from the current cached rows is heavily dominated by SWE-smith.

## Reproduction

```sh
PYTHONPATH=scripts .venv/bin/python scripts/report_image_readiness.py \
  --inventory build/cache-expansion-20261001/inventory.json \
  --catalog build/cache-expansion-20261001/aggregate-catalog-20261001T072720Z.json \
  --foundation-bundle image-campaigns/2026-09-30/inputs-projects-20261001.json.gz \
  --foundation-snapshot build/cache-expansion-20261001/foundation-capacity-20261001.json \
  --output /tmp/portfolio-readiness-new.json
```

The [full proposed source list](next-prep-20261001.json) is also saved locally in `build/portfolio-accounting-20261001/next-prep.json`, with selected source pins where available and unresolved-pin flags otherwise. The actual training recipe index remains unavailable; no sampler or training split has been changed.
