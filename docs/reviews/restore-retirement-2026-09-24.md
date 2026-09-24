# Restore cleanup candidate — 24 September 2026

Status: implemented and regression-tested; not deployed or load-qualified.
Production remains rc24. This selectively brings forward deferred checkpoint
retirement from `65e7947`, without the separate reclaim-ranking policy changes.

The rc24 sample measured synchronous artifact cleanup at 152 ms mean and 290 ms
maximum across seven wake traces. After the durable RUNNING commit, reflink
restores now leave this work to the existing five-second maintenance loop.
Durable overlap claims are the worklist; no additional queue or daemon is added.

Maintenance pins the allocation, checks lifecycle authority under the sandbox
lock, and unlinks only the exact superseded checkpoint outside that lock.
Immutable manifest identity and file identity are checked before deletion.
The manifest is removed last so interrupted cleanup can resume. Capacity stays
reserved until physical reclamation and reader closure permit release. A slow
old-generation unlink therefore does not block a later capture or wake. One
retirement lock is reused per incarnation to avoid accumulating a lock file per
checkpoint. Non-reflink cleanup keeps its existing behavior.

The existing aggregate restore-preparation timer is preserved, with separate
timers for workspace preparation, checkpoint validation, joining network
preparation, memory preparation, and source retention. These identify the next
target without weakening durability or integrity checks.

Validation: 134 focused local tests and Ruff passed; 205 focused Linux tests
passed against the candidate source in an isolated qualification directory.
Tests include blocked cleanup overlapping a subsequent park/wake, source-reader
retention, interrupted unlink/restart, failed restore, identity mismatch,
repeated-generation lock reuse, and memory-growth/admission regressions. Native
XFS behavior and loaded latency have not been requalified for this candidate.
The tests establish scheduling and ownership behavior, not a measured speedup.

Before deployment, run the same forced park/wake workload and a sustained
pressure run with native backing. Check wake latency, retirement backlog and
capacity recovery; delayed maintenance retains extra checkpoint space until it
catches up. This removes foreground waiting, not the underlying deletion I/O.
