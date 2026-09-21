# Avoiding publish-then-refetch

Subsequently deployed and verified as [release 0.5.71](release-0.5.71-production-2026-09-21.md).

Implemented locally on September 21, 2026; not committed or deployed. This
continues the [I/O opportunity review](storage-io-reduction-2026-09-21.md).

## Implemented behavior

Previously successful publication removed the local sealed inputs. The next
same-worker wake referenced remote blobs, even though logically equivalent data
had just been on that worker. The native remote cache might contain some of it,
but publishing did not guarantee that it was populated.

Publishers now expose the exact immutable inputs of confirmed dense exports to
the node service. After the publication commits, the service retains eligible
inputs by hardlink in a bounded cache keyed by blob origin and digest. A wake
creates mount-specific pins, journals those names before native device
acquisition, and uses `file` descriptors in its native source configuration.
Remote descriptors remain in the volume journal and in portable publications.

The retained original and its dense export can differ in physical layout, so
local files are never presented as byte-for-byte copies of the remote blob.
Only their logical equivalence is used. The pinned native backend already fences
premerged indexes by the actual layer index identity. Compacted uploads are not
mapped to a single input; a completed local compaction later uploaded as a dense
layer can be retained normally.

Retention and pinning add metadata operations but no data copy, reread for
hashing, or duplicate data allocation. Mount pin persistence is combined with
the existing pending-mount journal update rather than adding an unconditional
extra write transaction.

## Space, lifecycle and failure handling

The default idle retention budget is 4 GiB, capped at ten percent of configured
hard storage capacity. Allocated bytes, rather than sparse logical sizes, decide
eligibility. Entries are evicted by LRU; low physical headroom prevents retention
and reuse, and maintenance evicts entries when needed. The reserve is the smaller
of 1 GiB and five percent of filesystem capacity. Zero disables retention.

Mount pins survive idle-cache eviction and service restart. Safe release removes
them unless a retired device may still need the old stack. Reaping the last
retired device preserves a newer mount's pins while removing obsolete ones.
Exclusive reconciliation removes pins abandoned before journal persistence.
The idle budget does not include files still pinned by active/retired devices;
those cannot safely be evicted. Existing filesystem free-space admission remains
in effect. Cache unavailability falls back to remote descriptors, and retention
errors do not fail an already-committed publication.

Storage cache accounting now counts allocated bytes once per inode across cache
entries and volume pins. This avoids double-counting hardlinks and reporting a
sparse virtual EOF as allocated disk usage. New native-service metrics report
cache entries, retained bytes, hits, misses and evictions.

## Validation

Seven lifecycle/cache test methods cover Registry and S3 integration, no-copy
retention, eviction and restart with active pins, ordinary and retired-device
release, preserving newer mount pins, abandoned pin cleanup, allocation
accounting, low-space/disabled/unavailable caches, origin separation, remote
fallback, post-commit retention failure, and exclusion of compacted uploads.
The full Python suite passed: 1,042 tests with six environment-dependent skips.
Ruff and diff whitespace checks passed.

Native qualification used backend
`75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`
in an isolated temporary process on gateway job `12379311`, in UCloud project
`4827bd3a-4e74-4393-9b82-49f71636c141`. It created no block devices or mounts and
replaced no production service.

The fixture contained two published layers totaling 8,425,472 bytes and a 9 MiB
virtual image. Both original source names were removed and both idle cache
entries evicted. Native restacking through the surviving pins succeeded with an
unreachable remote URL and matched restacking the dense exports byte for byte
at the logical-image level. Overwrites, explicit zero writes, discards and holes
were preserved. Cache retention copied zero data bytes.

[Raw results](../benchmarks/published-local-cache-2026-09-21.json) and
[qualification script](../../runtime/storage_native/qualify_published_local_cache.py).
This verifies file-format and lifecycle behavior, not a production hit rate,
physical disk benchmark, or 256/512-sandbox throughput qualification.

## Other opportunities investigated

**Filesystem discard:** the pinned ublk target advertises writable-image discard
and forwards requests to the image. XFS mounts here omit discard and there is no
FITRIM workflow. However, the backend caps discard requests using its I/O buffer
size, so full-volume trim can become many individual operations. Enabling it on
every park could introduce significant work on the latency-sensitive path.
The inspected gateway has no ublk control device, and the bounded first-page
project job listing showed completed recent workers. No provider job was started
and no kernel module was loaded on the gateway. End-to-end XFS trim remains
unqualified and disabled. The next experiment should compare exported bytes
after create/delete/trim and verify native remount contents on an isolated
worker before selecting cadence and minimum extent size.

**Short-wait parking:** queued-response cancellation already avoids some work.
Adding a fixed delay would retain memory for all long model calls too, including
when other sandboxes are waiting to wake. No delay was enabled without a measured
short-response distribution and a pressure signal that can end the wait promptly.

**Maintenance scheduling:** local compaction is already coalesced per volume and
single-stream per node. A blanket stop at high I/O pressure can starve compaction
and make layer depth worse. A useful next experiment needs loaded-worker traces
to compare foreground latency and chain progress under a fair maintenance share;
this pass does not introduce a new pressure rejection or fixed concurrency cap.

After rollout, measure local-cache hit rate, avoided remote reads, filesystem
allocation, I/O pressure, and wake latency together. Retained files consume space
until eviction, and workloads larger than the cache may see little benefit.
