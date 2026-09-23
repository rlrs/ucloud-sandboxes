# Bound speculative read-ahead on guest memory volumes

The 0.5.102 rolling 256-agent run completed all 2,048 correct cycles, but wake
p95 was 22.396s and usable-tool p95 24.521s. The first worker accumulated 8,273
direct-compaction stalls while adjacent workers had zero. This is a loaded
worker problem that the soft working-set signal alone did not resolve.

XFS memory-file page faults appeared in earlier sampled compaction stacks.
The writable volumes now use one host page of read-ahead, set before each
mount, including pooled/reused and restored devices. Immutable Docker image
layers and host disk policy remain unchanged. Buffered upper I/O is retained.
This changes no checkpoint format, native binary, or durability fence.

On an idle UCloud worker, 128-KiB read-ahead yielded 2.049 GB/s buffered
sequential reads; disabling it entirely fell to 0.150 GB/s and was rejected.
One-page (4-KiB) and 16-KiB windows measured 1.339 and 1.368 GB/s respectively.
One-page random I/O measured 116,748 IOPS versus 132,032 in the baseline;
sequential writes measured 1.299 versus 1.270 GB/s. These short comparisons
show a tradeoff, not a universal throughput win. Both baseline and candidates
failed some existing within-15%-of-native-loop gates; do not call those gates
passed. The small-window stdout summaries survived; their raw JSON was lost
when normal idle autoscaling retired the test worker before collection.

Zero-read-ahead restore/metadata/ENOSPC/gVisor qualification passed. The exact
one-page setting still requires actual forced restore and dense-load validation.
38 Linux storage tests pass, including mount ordering and device-error handling.
The deployment must restart the idle Python storage service with the new wheel,
while preserving the native backend process; new workers use the new bundle.

This is a load-test candidate. The <1s end-to-end target remains unqualified.
