# Reduce repeated checkpoint writes

Implemented off production on 2026-09-22. No deployment, production mutations,
or production load tests were performed. These changes extend the uncommitted
[overload maintenance work](overload-maintenance-2026-09-22.md).

## Size-tiered compaction

The previous byte trigger flattened the entire chain once accumulated deltas
exceeded its budget. A large base or previously compacted delta could therefore
be rewritten repeatedly for small new checkpoints.

Selection now merges a suffix of comparable-sized layers. Older tiers join when
the accumulated suffix reaches their size, or when the existing depth target
requires it. A lone large delta is not rewritten merely for exceeding the byte
trigger. This applies to wholly local chains and to Registry/S3 publication.
Publishers retain already-published descriptors; every unpublished input is still
exported. Forced single-layer chains and changed blob origins retain full-merge
semantics. Local chains with remote bases retain their prior conservative policy.

Tests cover dominant bases, multiple retained tiers, growth into older tiers,
1,000 varied chain shapes, mixed local/published inputs, unchanged descriptor
ordering, and failed-export preservation of previous publications. No checkpoint
ownership or durability boundary was relaxed.

Tradeoff: keeping older tiers delays reclamation of overwritten base bytes and
may keep more layers than a full merge. The existing depth target and maintenance
disk reserve still apply. Subsequent native qualification passed on an authorized idle production worker;
see the follow-up below for measured synthetic-trace savings and limitations.

## Retain warm sandboxes while memory permits

The relay park grace budget now extends to 15 seconds with sufficient memory,
using the 90th percentile of recent response waits. It shrinks with memory
pressure, larger sandbox memory requirements, and queued/in-progress restore
memory demand. Demands are recorded before restore-slot acquisition and removed
on every exit, including admission failure; active reservations are deduplicated.
Queued cold starts and draining nodes end retention. Demand is checked every
50 ms, outside lifecycle locks. Explicit user parks remain immediate. The gateway
only forwards the request-bound lifecycle fields for durable relay dispatch,
where park/wake coordination supports crossed calls.

Tests cover demand arriving during retention, small versus large sandboxes,
response history exceeding the previous two-second budget, queued restore
visibility, demand cleanup, and avoiding double counting. No production latency
or total write reduction is asserted.

## Stop obsolete compaction promptly

Once deletion has durable authority, the service drops pending compaction and
signals any active export. Cancellation is checked before work, around output
pacing/writes, and by the native control-response progress callback. It therefore
interrupts the client waiter even before the first stream chunk. Closing the
export stream stops further local output; this does not promise instantaneous
cancellation of backend reads already in progress.

Metrics distinguish cancelled work and locally written bytes discarded before
candidate commit. Source pinning, owner checks, stream digest verification, and
journal-before-delete adoption remain unchanged. Tests cover deletion races and
cancellation before the first chunk without waiting for the 30-second timeout.

## Native qualification status

Two isolated Linux harnesses were added:

- `runtime/storage_native/benchmark_tiered_compaction.py`: starts a private daemon,
  compares actual exported bytes for the previous and tiered selectors across a
  synthetic repeated-overwrite trace, and checks logical contents after every
  append/merge, including zeros and discard markers. No block devices required.
- `runtime/storage_native/qualify_xfs_trim.py`: private 1 GiB XFS devices, deleted
  payload, retained files, zeros and holes; bounded 64 MiB FITRIM windows; full and
  suffix export, native remount and data verification. Requires meaningful export
  savings before reporting success. Never trims an existing sandbox.

**Automatic runtime trim is not implemented/enabled pending qualification.** The
available isolated host `rasmus-dev` runs Linux 5.15 and has no `/dev/ublk-control`.
The export backend also requires `IORING_SETUP_COOP_TASKRUN` and
`IORING_SETUP_SINGLE_ISSUER`; a direct syscall check succeeded with flags 0 but
returned EINVAL with those flags (4352), even with container seccomp disabled and
memlock unlimited. The attempted native export returned `channel closed` while
opening its first lower. Trim stopped at preflight without creating devices.

The attempted backend artifact was SHA-256
`c8035bd0d1c1bc0c3a76bbf261f854d82490efb40336c30038e0b1e1b86cb501`, pinned
AgentENV commit `db1492b7915a408b37f863c9e3a34b2ccb2fb1b0`, with earlier streaming
export/pool/owner patches. Repeat both harnesses with the exact deployment
artifact on a compatible idle non-production worker. A passed trim test would
still need concurrent latency/pressure measurements before adding automatic
incremental trimming. The user has been asked for a suitable worker.

## Verification

Python 3.13.12 in the isolated Linux Docker test environment:

- Full suite: **1,194 tests, 61 skipped, no failures**, 92.932 seconds.
- Additional cancellation regression and compaction module: **13 tests pass**.
- Ruff on touched implementation/tests/harnesses and `git diff --check`: pass.
- Native export and XFS trim qualification: **blocked by host kernel**, not passed.

[Evidence directory](../benchmarks/write-volume-2026-09-22/).

## Follow-up: production worker authorized and qualification passed

After the user authorized production use, both native harnesses passed on idle
worker 12398499 with the exact production artifact. Size-tiered compaction reduced
native export bytes by 87.48% in both a 32-cycle trace and four concurrent 64-cycle
traces. XFS trim removed 64.125 MiB of deleted payload from a full export and passed
full/partial native restore checks, including a second run with concurrent fsync
writes and reads. The kernel blocker above applied to the earlier development
host, not this completed qualification.

[Production-worker evidence and limitations](../benchmarks/write-volume-prod-2026-09-22/README.md).
Serving code remains unchanged and automatic trim remains disabled. These tests
are not a full sandbox load test and do not establish the wake-latency SLO.
