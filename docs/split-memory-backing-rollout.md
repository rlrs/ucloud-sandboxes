# Split memory backing rollout

The Warden exposes `workspace_record(sandbox)` for validated storage evidence and
`ensure_workspace_mounted(sandbox, operation_id=...)` for idempotent workspace
preparation. Import, restore, publication and node responses use this same public
contract. It resolves `workspace_volume_id`, validates the exact incarnation and
mount path, and grants no runtime execution authority. Callers retain their
existing lifecycle fences; only the journal's restore handoff starts execution.

Split memory backing separates the sandbox's mutable workspace block volume from
its ordinary-file gVisor memory backing. The worker has one XFS filesystem with
project quotas at the existing storage-native mount root. Workspace block mounts
and memory directories occupy distinct child paths. Memory backing does not add
one block device per sandbox.

This is opt-in: `sandbox.direct_split_memory_backing` defaults to `false`, including
older deployment configuration files which omit it. The generated node unit only
passes `--split-memory-backing` when enabled. Updating the worker package alone
does not enable the new layout. Builders reject this setting.

The worker command receives the same physical writable-byte budget already used
by the storage-native daemon. Full sandbox disk claims must be admitted
atomically in the direct registry before either component allocator runs, and
remain claimed while planned, importing or deleting. Component quotas are
secondary bounds, not two independent pools of physical capacity. Heartbeats add
native workspace reservations and memory reservations exactly once; they retain
the single native physical capacity. No second filesystem capacity/free value is
added to the node's advertised storage.

Memory checkpoint components use the existing worker registry origin and the
repository `<storage_native_repository>`. Ordinary workspace
snapshot backend selection remains unchanged. The registry must retain these
referenced checkpoint components until all owning generations and in-flight
imports release them.

Before enabling writers:

1. Deploy compatible checkpoint-component readers and ownership/recovery support
   across destination workers while keeping the flag off. Confirm that legacy
   combined-volume checkpoints still restore. A worker unable to read the new
   descriptor must not receive it through migration or replacement placement.
2. Qualify on a new, isolated worker. The opt-in bootstrap accepts a prepared
   XFS project-quota filesystem or creates one nodewide, owned sparse image on
   the same physical filesystem as the workspace backend. Creation requires an
   empty mount root, no previous workspace journal/runtime data, an exclusive
   new image and enough physical space for the shared budget plus metadata
   headroom. A durable receipt binds the image inode and UUID. Subsequent boots
   reattach it with verified direct loop I/O; they never reformat it. An
   interrupted format with allocated blocks requires explicit recovery.
   The canonical mount root is
   `/var/lib/ucloud-sandboxes/storage-native/mounts`; old worker paths are never
   covered or migrated implicitly.
3. Verify the actual pinned runsc memory-file behavior, sparse file allocation,
   quota exhaustion and release. Exercise cold and resident park, checkpoint
   publication, restart during allocation/publication/deletion, cross-worker
   import, legacy/new descriptor readers, and post-restart reservation accounting.
4. Run realistic density and dirty-page workloads. Record physical read/write
   bytes, PSI, device count, full reservation totals, checkpoint bytes and
   response-ready-to-useful-tool latency. Merely passing configuration/unit tests
   is not the runtime qualification.
5. Enable the flag for qualified new workers, then drain existing workers through
   the normal ownership protocol. Disabling the flag stops new split writers;
   keep compatible readers and files available until split owners are gone.

Configuration/CLI/bootstrap/heartbeat tests are in
`tests/test_split_memory_wiring.py`. They verify default-off compatibility,
explicit argument forwarding, safe filesystem provisioning, one shared capacity
and live additive reservation accounting. `tests/test_memory_filesystem.py`
exercises restart reuse, refusal of existing worker data, interrupted formatting,
changed image identity and rejection of buffered loop I/O. These
tests do not authorize or perform production activation.

Native evidence is retained in `docs/benchmarks/split-memory-2026-09-23/`.
The product lifecycle fixture uses the real Warden, storage Unix protocol,
ublk/XFS workspace and OverlayRootfsManager. It enforces a kernel project quota,
rolls an injected pre-COMPLETE failure back to the exact original process, then
checks memory, timers, TCP connections and persistent file descriptors across
three capture/restore cycles. Single-sandbox wake was 270–275 ms; this does not
establish a density or production p95 claim. Remote transport and promotion
qualification remain separate gates.

The allocator retains its hard claim while publication holds source files open.
Deleting an allocation with a reader marks it deleting and defers reclamation;
only closing the reader and removing both backing components releases the direct
registry's physical claim. A migration reimport gets a new local project ID while
retaining its portable allocation identity.

### RAM-active qualification

The optional RAM backend retains the same v3 checkpoint contract and quota-owned
persistent memory component. Live `application_memory.active` resides on a
bounded, non-swapping tmpfs. The runtime sparsely exports its allocated ranges
after the capture barrier. Abort resumes the original RAM-backed process;
committed capture still requires the single durable COMPLETE pointer before
reaping it. Restore populates a separate tmpfs file **inside the sentry's cgroup**,
then binds that file to the restored allocator. A node-agent-side copy would
charge the wrong cgroup and is deliberately excluded.

The sixth pinned runtime patch and complete companion build manifest are required
before selecting this writer. Its boot fingerprint is distinct from persistent
file backing. Native lifecycle, capture abort, three subsequent restores, and
remote OCI import passed. A 1.5 GiB guest heap was charged as 1,536.7 MiB of shmem
in its sandbox cgroup, with total sandbox usage around 1,565 MiB. No disk-backed
reclaim was requested for those shmem pages. These are functional and accounting
results; aggregate throughput and density still require the load gates.

For sparse XFS images, deletion retains the allocation's hard claim until a
successful nodewide trim returns the image's free extents to the parent physical
filesystem. Concurrent deletions share serialized trim passes. An actual 64 MiB
write/delete qualification returned 67,276,800 physical bytes before releasing
the claim; failures retain it for retry.

The final large-heap fault qualification restored a 1.5 GiB guest with TCP,
timers, child processes, Unix sockets and persistent file descriptors across
three captures. A deliberate 128 MiB candidate cgroup limit caused a real OOM
kill during restore. The original complete checkpoint remained authoritative;
reconciliation and retry under the original 2 GiB limit succeeded. Full cold
restore of that heap took approximately 1.35 seconds on the isolated worker;
this is not a sub-second cold-wake claim. Retaining resident model waits avoids
that copy entirely.

The qualification also caught and fixed two recovery edges: `runsc list` returns
JSON `null` for an empty runtime, and a checkpoint process can die after pausing
the original but before creating its export file. Recovery now treats the empty
inventory correctly and verifies the original process identity and RUNNING state
before aborting its capture journal. File existence never grants execution.

### Demand-paged file restore and retained-runtime reclaim

The seventh native patch adds optional private XFS-clone restore. Fresh owners
may remain RAM-backed; at a fenced parked restore the Warden can persistently
select file backing. The candidate receives a private clone, retaining the
original complete checkpoint until durable RUNNING. A separate retention
project and the existing registry ledger account for temporary source overlap;
the live project's quota is not increased. Converted allocations require a
compatible reader until drained, including after disabling new conversion.

File-backed application pages can be explicitly written back and reclaimed
without checkpointing or changing the live runtime identity. The policy uses
actual achieved memory progress and the existing wait/cancellation fences. A
full capture remains available when retaining the runtime cannot satisfy the
required pressure relief. This adds no quiesced lifecycle state.

[Native and assembled product qualification](benchmarks/memory-tiers-2026-09-23/README.md)
passed full-heap/TCP/SQLite, abort, failed candidate, quota transfer, owner-mode
and cleanup checks. Demand paging moves read cost to guest page access; the
single-guest result does not establish fleet-level pressure latency or density.
