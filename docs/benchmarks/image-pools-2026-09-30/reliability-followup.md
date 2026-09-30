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

The implementation and focused tests are complete. The helper files are staged on the gateway, but full source/foundation canaries and activation in the running coordinators remain unverified because the forwarded SSH agent stopped responding. The running coordinators retain the previously deployed improvements. Do not claim the final prelink rollout is complete without its canary receipts and queue restart verification.

## Coverage and recovery

At 14:47:28 UTC, 544 source images were ready. Ready TMax prefixes covered 12,329 of 14,600 raw recipes, including the explicit shared foundation; ready Terminal prefixes covered 7,278 of 13,825 recipes. These prefixes still leave task-specific steps. Registry use was 1,461,085,716,480 bytes. The larger task-image inventory remains 35,984 references, including 35,890 task-specific upstream images. Docker Hub first-pull limits and large, flattened images remain material constraints; this is not broad task-image readiness.

The refreshed portable bundle preserves 451 source digest pins and the unchanged 8,602 foundation contexts. All contexts materialized and validated offline. Recovery now stages the 18 deduplicated bases before foundation commands, with local-base reuse and source staging enabled. Unpinned package repositories still prevent a byte-identical rebuild guarantee.

The combined focused suites ran 107 tests, with two privileged filesystem fixtures skipped. Live sandbox checks complement these tests. The actual training index still needs rewriting, auditing and qualification against the final ready catalogs.
