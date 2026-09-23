# Hybrid memory under physical pressure

The memory/workspace split alone did not satisfy P3/P5 of the original performance
plan. RAM-backed execution avoids continuous heap writeback, but its full park
copies the allocated heap to disk and restores it eagerly into tmpfs. The rc18
256-sandbox integrity run completed 2,048 cycles without a recorded SIGBUS, yet
its continuation p95 was 53.4 seconds and fleet resource probes failed. That is
not performance acceptance.

## One lifecycle, two backing placements

Fresh owners can retain RAM backing. Once an owner has durably parked, qualified
reflink restore selects file backing monotonically for that incarnation. This
selection commits before admission quotes restore demand and survives failed
candidates and process restarts. It does not introduce another execution state
machine or grant execution authority: the Warden journal still owns that decision.

File-backed application memory can then remain in its running runtime across a
model wait. Reclaim writes back the owned application-memory file, rechecks the
wait and process identity, and reclaims measured cache pages in cancellable
windows. The application allocation lease remains held until the writeback FD
closes. The lifecycle lock is not held over writeback. Workspace capture and a
replacement runtime are unnecessary for this path.

A real physical deficit sizes reclaim. Existing policy accounts for in-flight
byte promises across owners, and treats expected application-memory refaults
separately from generic filesystem-cache thrashing. File-backed reclaim can make
progress under storage pressure instead of falling through to a full checkpoint.
RAM-backed owners keep their full physical/tmpfs growth guarantees. File-backed
restores temporarily reserve the full declared physical bound, with no fictitious
tmpfs demand; later reclaimable file growth does not create permanent virtual-heap
debt. This is conservative restore admission, not a claim that native resident
memory always equals its small initial checkpoint footprint.

## Immutable restore and quota ownership

Native restore creates a private reflink of the immutable application-memory
checkpoint, restores into that candidate while paused, and preserves the source
until the existing RUNNING handoff commits. It never falls back to a whole-file
copy or modifies the retained source after a failed candidate.

XFS charges reflink extents to both inodes. The canonical direct registry therefore
reserves the rounded allocated source bytes, in the same capacity transaction
used by workspace/memory admission. Before clone, the allocator durably records
a separate project ID and assigns only the immutable main-memory inode to that
exact-size retention project. Cross-project reflink on the same XFS filesystem
has been verified. The live application's original hard quota never increases.

Retirement requires artifact removal, closed allocation readers and physical
reconciliation before releasing the project and registry claim. A failed native
restore keeps its source and claim for an idempotent retry. RUNNING recovery
checks retained claims even after artifact inventory is empty. Allocation deletion
and post-delete claim cleanup are ordered, crash-recoverable steps.

Memory journal version 2 and direct registry schema 6 fence old readers. Disabling
the reflink capability while retained allocations, retention projects or overlap
claims exist is rejected; downgrade requires draining these owners. The immutable
rootfs image path remains separately opt-in.

## Evidence and remaining acceptance

The isolated native 1.5 GiB heap / 384 MiB dirty-turn experiment verifies all heap
words, an established TCP connection and SQLite WAL state. Private file restore
starts with roughly 25 MiB resident memory, rather than eagerly populating the
1.5 GiB heap. Native source mutation/failure/retry and XFS project-boundary checks
are separate from the Python integration tests.

Python qualification covers admission ordering with zero available tmpfs, shared
reclaim demand, foreground cancellation, source retention through failed candidates,
post-RUNNING cleanup failure, restart, deletion, reader leases and rollback fencing.
Full regression and production pressure results must be recorded with the exact
candidate. Isolated timings do not establish loaded end-to-end latency. The
original sub-second continuation/tool goals remain open until that load passes.

## rc19 loaded result and rc20 corrections

The rc19 forced-park smoke completed all 64 cycles and all 16 primary processes
normally, including full heap/TCP/SQLite checks. Continuation p95 was 1.221 s and
useful-tool p95 1.719 s, so it did not satisfy the latency target. Sampled traces
attributed 58–309 ms of wake to artifact cleanup, including physical retirement.

The default-budget 256-sandbox pressure run completed 966 cycles before sandbox
0168 hit its 180-second primary-start timeout. Continuation p95 was 18.525 s and
useful-tool p95 25.887 s; three fleet freshness checks failed on worker 12400715.
No SIGBUS or OOM was found in the collected evidence, but this was not a passed
correctness run. Cleanup finished with no active routes or pending delivery.

The placement evidence shows a concrete imbalance: stale startup CPU samples
excluded three workers, so the fourth grew to 73 owners while peers had 55, 64
and 64. PostgreSQL had an initial 3.68-second WAL stall but no sustained later
stall in the sampled evidence. That does not explain the full pressure tail.

The rc20 candidate keeps assigned resource shape first in create placement and
makes CPU load advisory there. Worker admission retains the existing FIFO
startup/restore queue and physical/tmpfs/storage guards. Recovery now selects
file placement for a recovered PARKED owner before quoting restore demand.
Physical memory-project retirement moves to the existing maintenance loop,
which batches one trim without holding lifecycle locks and retains capacity
claims until exact physical cleanup completes. Artifact unlink remains fenced
on the wake path. Fresh-heartbeat empty relay polls use read-only PostgreSQL
transactions; actual work still takes the atomic registration/lease locks.

These corrections require fresh loaded qualification. Neither the native
microbenchmarks nor the rc20 unit/PG gates establish sub-second fleet latency.
