# Worker I/O and reclaim optimizations — September 21, 2026

These changes address avoidable snapshot I/O and placement concentration. They
are implemented in the shared Python control plane and storage publishers; the
native storage and gVisor binaries are unchanged. They are not yet deployed or
qualified with a new production load run.

## Evidence and scope

The retained 11:02 UTC production heartbeat for worker 12397674 reported 17
snapshot publications, eight compactions, and 38,305,353,728 uploaded bytes.
An eight-second host sample showed roughly 304 MB/s physical reads and 167 MB/s
writes, I/O PSI `some` around 36%, extensive reclaim and filesystem/block waits.
Other workers were less busy. Individual 1-GiB cgroups also hit their memory
limits. Publication counters are cumulative; these observations do not isolate
how much of the sampled I/O each mechanism caused.

Inspection found that create placement ranked image locality, in-flight creates,
and resource packing, without considering I/O PSI. Optional wake consolidation
also lacked an I/O comparison between source and destination. These decisions
could favor older, more occupied nodes even when another warm worker had much
less storage contention.

## Less full-chain rewriting

Both Registry and S3 publication used total chain bytes to trigger compaction.
If an already compacted base was larger than the 4-GiB default, every subsequent
tiny update could trigger another full flatten and upload. The byte trigger now
counts accumulated deltas after the oldest base. The independent eight-layer
trigger still bounds lookup depth, and changing blob origin still forces
compaction. This changes the meaning of `snapshot_compact_after_bytes` from total
chain bytes to delta bytes; it is a maintenance trigger, not a storage quota.

Local layers also used `st_size`. In the supported sparse upper mode, the native
implementation retains the virtual volume EOF when sealing, so a small written
delta can look like many GiB. The new estimate uses the smaller of logical size
and allocated bytes (`st_blocks * 512`), falling back to logical size where block
accounting is unavailable. The current default is hybrid-log-structured; this
sparse-mode issue has not been established as a cause on worker 12397674.

Allocation is only a compaction-cost estimate. Dense export still computes and
validates the exact streamed size/digest. Publication commits, prior snapshot
retention, hard disk reservations, and failure recovery are unchanged. These
changes avoid unnecessary old-layer reads, writes, uploads and associated cache
pressure; they do not change application memory limits or kernel reclaim policy.

## Placement and optional consolidation

Workers now sample Linux I/O PSI `some` and `full` ten-second averages and include
them in heartbeats. I/O stalls and partial memory-reclaim stalls participate in
the existing normalized pressure score. Create placement combines that score
with in-flight creates divided by the existing per-node create target, before
resource packing. Immediate route reservations therefore counteract stale
heartbeat samples during a burst. Image locality remains preferred; this does
not claim that a cold node always beats a busy warm node.

Optional consolidation skips destinations with worse measured I/O or partial
memory-reclaim stalls than the source. It then wakes locally through the normal
path. This comparison does not reject client work or initiate additional
migrations. Existing locality, admission, capacity, identity and migration fences
remain in force. Pressure is a relative placement signal, not a new rejection
threshold or sandbox-count limit. These changes influence new work and optional
moves; they do not forcibly relocate running sandboxes.

The gateway accepts canonical persisted heartbeats without the new optional
fields, including workers with nonzero device-count overrides. Unknown fields,
invalid types, and corrupt/noncanonical rows remain rejected. Upgrade the gateway
before workers: an older strict-schema gateway does not recognize the new
metrics. No SDK or Verifiers update is required.

## Verification

Publisher regressions exercise both backends with real Unix-socket export
streams and in-memory object stores. A base larger than a scaled-down byte
threshold plus three sparse small deltas appends without compaction; every
result verifies its manifest and layer descriptors. A fourth delta triggers the
configured depth bound. A separate test confirms that accumulated delta bytes
still trigger compaction below that depth. Existing origin-switch, upload
failure, publication and recovery tests remain relevant. These are protocol and
policy checks, not a native-filesystem throughput benchmark.

Placement tests use two warm workers, one with 75% I/O PSI and tighter disk
packing. The first create selects the quiet worker; an eight-create burst favors
that worker but spreads as its in-flight reservations accumulate. A lone worker
at 99% I/O PSI remains eligible under the existing admission checks. Additional
tests cover PSI sampling, legacy persisted rows, partial reclaim ranking and
consolidation direction.

The canonical repository check passed: 999 server tests (six platform skips),
118 SDK tests, Ruff, shell syntax checks, Go tests, wheel builds and isolated
install checks. Shellcheck is unavailable on this host and was explicitly
skipped through the supported check-script override. Production impact remains
to be measured after gateway-first deployment and worker rollout.
