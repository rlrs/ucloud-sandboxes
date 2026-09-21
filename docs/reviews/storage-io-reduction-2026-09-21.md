# Further storage I/O reduction

Source review and local implementation on September 21, 2026. These changes are
not committed or deployed. The production bottleneck evidence remains the
[earlier measurement](production-performance-2026-09-21-afternoon.md); this pass
does not claim a new production profile or measured throughput gain.

## Implemented: reuse completed uploads after publication failure

Previously, successful layer uploads were forgotten if a later layer or final
snapshot metadata commit failed. Retrying an unchanged chain reran native dense
exports or compaction and uploaded the resulting bytes again. Both Registry and
S3 now retain a bounded in-memory index of completed exports scoped to the
publisher/backend. This includes compacted outputs when their complete input
stack and configuration match.

Reuse checks path, device, inode, size, modification time and change time for
each immutable local input. Compacted exports additionally key their remote
descriptors, source origin and global configuration identity. Inputs are checked
again after remote lookup or upload. Hits require the remote content-addressed
blob still to exist; S3 also checks its size. Missing blobs are exported again.
Ownership checks still run around reuse and before metadata commit. This avoids
hashing the entire source again merely to determine whether it is reusable.

Only confirmed completed uploads enter the index. It does not preserve partial
streams or replace journal authority. Eviction or service restart merely loses
the optimization. The metadata cache retains at most 1,024 entries per publisher;
this is a memory bound and never an admission limit on sandbox operations.

New `snapshot_reused_layers` and `snapshot_reused_layer_bytes` metrics count
avoided exports/uploads. Successful-publication uploaded-byte counts exclude
reused outputs; as previously, that existing metric omits failed attempts.

Validation passed 1,035 tests with six environment-dependent skips, plus Ruff
and diff whitespace checks. Six added regression methods exercise both backends,
dense and compacted retries after metadata failure, missing remote objects,
changed inputs/configuration, later-layer failure followed by an appended delta,
ownership loss during a cache hit, input mutation, and eviction. Retry tests
explicitly prohibit another native export, and verify the resulting published
snapshot. These are correctness and avoided-work checks, not disk-throughput
benchmarks.

## Larger remaining candidates

1. **Propagate filesystem free space into block snapshots.**
   `LinuxStorageHostOperations.mount` uses XFS `noatime,nouuid`; the repository has no
   `fstrim`/FITRIM path. The native format supports discard mappings, but the
   existing layer tests do not establish end-to-end XFS-to-ublk discard behavior.
   Deleted files may therefore leave mapped blocks that continue to be exported
   and merged. Qualify create/write/delete/trim/seal/restore on an isolated device,
   measure exported bytes, and verify discard behavior through a retained base.
   Do not add synchronous full-filesystem trim to every park without measuring
   its latency and lock contention.
2. **Avoid publish-then-refetch on the same worker.**
   Successful publication clears sealed paths and removes their local files;
   the next source configuration refers to remote published descriptors.
   Quantify backend cache misses after local wake. If significant, preserve or
   seed a bounded verified local cache of published data while giving disk
   pressure eviction priority. Retained files must be associated with the exact
   exported content: a sparse sealed input is not automatically the same byte
   representation as its dense uploaded blob.
   Implemented and natively qualified in the subsequent
   [local reuse pass](published-local-cache-2026-09-21.md), using logical local
   equivalents without mislabeling their bytes as the remote digest.
3. **Avoid checkpointing short waits.**
   Existing queued-park cancellation saves checkpoints when a response arrives
   before dispatch. A short, pressure-aware delay before dispatch could also
   save park/wake I/O when model responses finish quickly. It needs a measured
   memory cost and must respond promptly to actual memory pressure; a fixed
   delay could reduce available capacity.
4. **Coordinate maintenance with foreground I/O pressure.**
   Local compaction and publications for other volumes can still overlap wakes.
   Prefer foreground restores while background jobs accumulate enough deltas to
   make merging worthwhile, and retain fairness so deferred chains eventually
   compact. This is a scheduling and latency improvement; by itself, moving work
   to another time does not reduce total bytes.

The current gVisor external-backing save path already saves allocator metadata
without scanning or copying all memory contents. Required artifact fsyncs still
flush dirty state before checkpoint authority transfers; removing those flushes
would weaken recovery rather than eliminate logically unnecessary work.
