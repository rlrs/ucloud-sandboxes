# Image preparation reliability follow-up

The [measured results](reliability-followup.json) describe live preparation on September 30. They are snapshots of an ongoing campaign, not qualification of an unavailable training selection.

## Deployed changes

- All eight builders release temporary local publication tags after finishing. Rootfs collection previously removed mounts and reader pins but retained the pulled tags and their Docker layers. Several 160 GiB Docker filesystems filled despite registry headroom. Idle, fenced cleanup reclaimed the completed precomputation images; the future-builder bundle contains the fix. One completely full filesystem required 4 GiB of logical headroom inside its existing root disk before Docker could write deletion metadata. Its final cleanup reclaimed 154.5 GB. No provider volume was enlarged.
- Gateway dispatch consolidates light traffic up to two builds per pipeline-aware builder, then uses idle peers and balances busy peers. Atomic admission and the hard concurrency ceilings remain in place. Legacy builders retain their prior selection behavior.
- Both large foundation queues use exact digest-matched prepared bases. All 8,590 TMax-inline and Terminal plans have matching local bases. The complete 8,602-foundation recovery bundle depends on just 18 immutable base images.
- Source preparations stage each admitted upstream image into the existing registry, reusing retained blobs. An expired blob token gets one refresh; repeated denial remains an error. A large interrupted source resumed with 17,870,924,827 bytes reused and only 192 bytes downloaded.
- Terminal preparation concurrency increased from four to eight, using the existing eight-builder ceiling. Source and TMax coordinators also use eight workers. The registry remains 3,000 provider GB, with the 500 GiB free-space reserve and existing 1,200/1,800 GiB observed-growth limits.

## Observed latency

The earlier cohort contains 195 successful builds starting between 14:12 and 14:27 UTC. The later cohort contains 113 successful builds starting after 14:34:30 UTC, observed at 14:45:16 UTC. Different recipes, cache warmth and available builders prevent treating this as a controlled causal benchmark. The later cohort had no failed builds at that observation; 19 were still running.

| Metric | Earlier | Later |
| --- | ---: | ---: |
| Publication queue median | 13.410 s | 0.002 s |
| Publication queue p95 | 105.048 s | 18.151 s |
| End-to-end successful build median | 77.096 s | 61.489 s |
| End-to-end successful build p95 | 166.893 s | 141.734 s |

Earlier remaining failures included a Debian package 404 and the old full builder before its final recovery. Recipe failures and old failed receipts are explicit coverage gaps, not ready artifacts.

## Avoiding repeated registry uploads

A large source demonstrated that BuildKit uploaded an already-staged 16,929,914,030-byte layer again during cache export, taking about 130 seconds. The new preparer code pre-links existing base blobs into the managed output and cache repositories before submission. A live probe linked that exact layer into a fresh output repository and the cache in 24.385 ms, with no layer upload and verified destination availability. This probe establishes repository reuse, not an end-to-end speedup for the whole build.

SSH recovered and both full canaries passed on September 30 at 21:12 CEST (19:12 UTC), including sandbox checks. The source canary reused 334,019,346 bytes without upstream downloads, linked 22 layer descriptors across the output/cache repositories in 243.744 ms, and completed its accepted build in 1.183 s. It reused all four EROFS groups and skipped the Docker pull. The foundation canary linked eight descriptors in 113.563 ms and completed its accepted build in 29.663 s, then passed its sandbox check. Both ran on a newly provisioned builder. These accepted-build timings exclude provisioning and subsequent sandbox validation; the complete canary processes each took about three minutes.

The deployed preparer scripts now contain the verified optimization. The three bulk queues had already exhausted their configured storage-growth allowances and remain stopped. Future invocations use the new scripts; there is no running old coordinator awaiting activation. Canary evidence is included in the measured-results JSON.

## Coverage and recovery

At 14:47:28 UTC, 544 source images were ready. Ready TMax prefixes covered 12,329 of 14,600 raw recipes, including the explicit shared foundation; ready Terminal prefixes covered 7,278 of 13,825 recipes. These prefixes still leave task-specific steps. Registry use was 1,461,085,716,480 bytes. The larger task-image inventory remains 35,984 references, including 35,890 task-specific upstream images. Docker Hub first-pull limits and large, flattened images remain material constraints; this is not broad task-image readiness.

The refreshed portable bundle preserves 451 source digest pins and the unchanged 8,602 foundation contexts. All contexts materialized and validated offline. Recovery now stages the 18 deduplicated bases before foundation commands, with local-base reuse and source staging enabled. Unpinned package repositories still prevent a byte-identical rebuild guarantee.

The combined focused suites ran 107 tests, with two privileged filesystem fixtures skipped. Live sandbox checks complement these tests. The actual training index still needs rewriting, auditing and qualification against the final ready catalogs.

## Reconnected status, September 30, 21:07 CEST

The completed bulk run reached 671 ready source images, 1,239 TMax inline foundations and 1,957 Terminal foundations. With the previously prepared explicit TMax foundation, ready prefixes cover 12,956/14,600 TMax recipes (88.7%) and 8,683/13,825 Terminal recipes (62.8%). These are prefix-coverage figures, not completed task-image percentages. Counts exclude the new proof artifacts.

Registry use was 2,379,658,756,096 bytes, with 639,488,782,336 bytes available on the unchanged 3,000 provider-GB volume. The 500 GiB reserve accounts for 536,870,912,000 of those available bytes. All bulk coordinators reached their persisted batch-growth budgets; their exit code is 1 because deferred entries are reported as incomplete work. This was not a gateway crash or a full registry. The public service, gateway and autoscaler were healthy, and registry maintenance timers were enabled.

The TMax catalog has 1,533 storage-deferred recipes and 14 failed recipes; Terminal has 3,839 storage-deferred recipes and eight failures. Failures include old server execution deadlines and upstream package/download errors. One large MONAI source finished its upstream staging but failed filesystem publication with `invalid authenticated environment range index`; it is not ready. The current component format permits at most 65,536 chunks of 256 KiB (16 GiB per EROFS component). The receipt alone does not prove which range-index invariant failed, so a size-limit diagnosis remains unconfirmed.

## Complete generic bases and preserved filesystem prefixes

On September 30 at 21:34 CEST, the 21 missing generic bases completed preparation, bringing the inventoried generic base set to **94/94**. They had previously been deferred behind task-specific images despite their small aggregate size. A subsequent eight-concurrent-sandbox qualification passed all 94 images in 35.015 s with no new import builds; create p50 was 1.769 s, p95 3.606 s and maximum 3.868 s. Ninety-two checks used original source aliases; the two Python slim references with different pre-existing aliases used explicit prepared image IDs. Rewrite recipes to pinned catalog references to select the exact qualified artifacts. Existing aliases were not overwritten.

Planning now places generic bases ahead of the task-image tail regardless of fanout. Recovery has a separate 94-base stage, and the new portable bundle preserves 693 immutable source pins with the original 8,602 foundation contexts. The index audit separately rejects unqualified remaining RUN/COPY/ADD work by default. A prepared base alone does not qualify package installation, network access, compilation or arbitrary task scripts as cheap.

An exact-prefix audit found 647 unfinished Terminal foundations beginning with already prepared dependency recipes. The preparer can substitute the longest byte-matched, validated prefix while preserving canonical inputs and the remaining instruction order. Two real recipes passed both sandbox checks and functional checks (NumPy import and Apache rewrite-module enablement).

Those canaries exposed a separate storage inefficiency: the 64 MiB layer grouping threshold repacked a small prepared base together with a tiny task delta. The builder now probes for existing signed shorter prefix components on group misses, with at most 16 optional probes and a shared one-second budget, while preserving the composition limit, parent-chain checks, signatures and retention refresh. Both selective materialization and the Docker fallback preserve the matched component boundary.

Fresh-builder canaries extended the prepared NumPy and Apache images with a small file. Each published only **4,096 new EROFS bytes**, reused one existing component, fetched 156 compressed layer bytes and skipped Docker pulling. Accepted-build times were 5.898 s and 5.156 s; filesystem publication was 0.865 s and 0.745 s. Package/module behavior and the new file were verified in real sandboxes. These are tiny-delta canaries, not a performance forecast for arbitrary installations; the earlier recipes had different deltas and are not a controlled timing baseline.

The builder wheel and future-builder bundle are deployed (wheel SHA-256 `2ccf7e0f4da530dc2567dec99378e13aaed35fefe3912aba4fd458a4e8c9c4b6`; bundle SHA-256 `5be01496fb3142f7e57b3b74b470cd3e4d53b50ee75d1a259bd718ef2db5e44d`). Native binaries, dependency versions and the sandbox-worker bundle are unchanged. All previous builders had scaled down before switching provisioning; the canaries exercised a newly provisioned builder. The focused suites passed 126 tests with two privileged fixtures skipped. The remaining 645 exact-prefix candidates have a bounded four-worker campaign with 24 GiB growth allowance and the unchanged 500 GiB reserve; scheduled candidates are not counted ready.

The complete source catalog now covers 692 references. **This does not meet the broader goal of every task needing only small live steps.** Many SWE inputs are task-specific images, including flattened sources with no shared OCI layers in the inspected samples. Their coverage remains sparse, and some prepared generic bases still leave expensive dependency commands. Generic-base completion must not be presented as completion of all SWE task bases or dependency foundations. BIRD/NeMo and the actual training selection remain outside this inventory.

At 21:58 CEST, the new batch had 161 validated foundations and 162 successful build receipts, with no recorded failures. It had built 1,724,665,856 EROFS bytes, reused 238 groups, and skipped Docker pulling in all 162 builds. Accepted-build p50 was 5.231 s and p95 14.593 s. Observed total registry growth was 2,782,715,904 bytes, including OCI/cache/publication overhead; this is an ongoing selected batch, not a randomized comparison with the earlier campaign.

A read-only comparison of retained ScaleSWE `adamchainz_apig-wsgi_pr80` and `adamchainz_apig-wsgi_pr93` found 1,133,885,082 regular-file bytes at matching paths with identical recorded tar metadata and contents, out of 1,150,588,053 regular-file bytes in the second image (98.55%). The two flat OCI layers share no layer identity. This identifies file-level sharing as a useful next investigation; it does not establish physical savings, safe rebasing semantics (including hardlinks/whiteouts), or a corpus-wide ratio. No file-level rebase or deletion of existing image content was performed.
