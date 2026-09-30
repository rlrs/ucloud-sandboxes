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
