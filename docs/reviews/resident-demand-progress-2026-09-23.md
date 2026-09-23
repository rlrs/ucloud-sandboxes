# Resident demand and continuation priority

Status: implemented after rc18, not production-qualified. The unchanged rc18
pressure-integrity run completed all 2,048 cycles but had 594 measured parks and
roughly 54-second continuation/useful-action p95. This change does not claim to
solve the cost of copying RAM-backed checkpoints; native backing alternatives
remain a separate qualification.

## Cause and change

The old reclaim forecast filled every configured startup and restore slot before
work could allocate. With eight slots of each kind and 2 GiB pending owners, it
could request 32 GiB of headroom solely for a queue. The new forecast keeps every
admitted growth/transition claim, deduplicated by exact incarnation, and adds only
the next eligible FIFO owner. Pending restores precede new startups. Each grant
updates the ledger before another admission decision. This bounds speculative
reclaim without limiting concurrent work that already fits.

New startup admission protects the next queued restore's memory only when their
combined demand would exceed actual headroom. The restored owner's full bound is
unchanged; the same owner is not charged twice across growth and actual restore.
Existing queues, deadlines, deletion/drain cancellation and lifecycle fences remain
authoritative. This is not strict end-to-end FIFO across transport retries.

Response arrival is recorded in the existing wait-observation metadata. A wait
whose response is ready ranks behind other safe waits, but remains reclaimable
when all waits are response-ready. Admission still succeeds before the durable
wake fence cancels capture. Observed model-wait duration now ends at response
arrival, excluding our own admission delay; retries cannot inflate that history.

## Evidence and rollout

The retained rc18 report has 233 node samples reclaiming for queued demand;
225 had over 10 GiB physical memory available (median 15.18 GiB), and 201 had
active checkpoints. This is evidence of substantial headroom during reclamation,
not proof that every such capture was unnecessary: the old telemetry did not
separate admitted growth from queued costs. See the
[sample summary](../benchmarks/resident-demand-2026-09-23/rc18-demand-samples.json).

The resident-wait heartbeat now reports `admitted_demand_bytes`, the incremental
`pending_demand_bytes`, and `unknown_transition_memory_costs`, all derived from the
existing ledger. Admitted bytes are known forecasts, not measured RSS; the unknown
count preserves missing cost evidence. Old worker reports decode these fields as
null. Deploy gateway readers before workers emitting them.

The focused Linux gate passed **128 tests in 8.799 seconds**, including full-bound
crossed capture, concurrent admission, protected restore headroom, ample-headroom
parallelism, deletion/drain cleanup, unknown memory, response priority and history.
The durable heartbeat upgrade lane passed **10 tests in 0.481 seconds**. Raw logs
are retained in [the qualification directory](../benchmarks/resident-demand-2026-09-23/).
A matching loaded A/B is required before attributing fewer parks, lower I/O or
better continuation latency to these changes.

## Hybrid backing admission and retained application memory

The next tranche separates physical memory and unswappable RAM-backing demand in
the same transition ledger. RAM owners retain their complete future-growth
guarantee in both resources. A file-backed restore still reserves the full managed
memory bound temporarily: measured native fixture costs are not universal bounds
for private restore allocations. It consumes no tmpfs claim. After the first
authenticated model wait, a file-backed owner's evictable application heap is no
longer charged as permanent unswappable growth debt. Its unmeasured private growth
remains explicitly unknown in telemetry; this does not assert zero native cost.
Initial primary launch remains bounded until that first safe wait.

The Warden commits a one-way RAM-to-file choice only for a durable parked owner.
Recovered or imported checkpoints pass the same fenced preparation before
admission quotes their cost. A nonblocking preparation probe lets a response join
admission while an existing capture completes; actual restore still revalidates
the exact checkpoint. File-page eviction cannot satisfy a tmpfs-space deficit.
The heartbeat adds separate admitted and pending RAM-backing bytes, with absent
fields remaining unknown for old workers.

For explicitly enabled file-backed application heaps, resident reclaim targets
the measured physical deficit and measured non-shmem file pages rather than the
generic 256 MiB cache probe. The Warden flushes only the owned active application
file; service ownership and wait checks bracket that operation. Refreshed cgroup
identity and clean-byte measurements then govern existing 16 MiB kernel reclaim
windows. Response arrival, changed ownership, or recovered headroom cancels
further work. Known application-file reclaim remains eligible for a real physical
deficit under storage pressure, since falling through to a full checkpoint writes
more data; the existing shared byte budget still bounds concurrent flush owners.
Achieved bytes alone count as progress. Expected
heap refault on the next turn does not trigger the generic cache's full-checkpoint
backoff. The feature-disabled RAM and legacy cache paths retain their behavior.

The assembled hybrid lane passed **176 Linux tests in 15.425 seconds**
([raw log](../benchmarks/resident-demand-2026-09-23/hybrid-resident-linux-tests.log)).
The Python 3.10 compatibility lane passed **91 tests in 8.168 seconds**
([raw log](../benchmarks/resident-demand-2026-09-23/hybrid-resident-python310.log)).
Tests cover real allocation/Warden-journal RAM-to-file selection
with exhausted tmpfs, writeback cancellation, cgroup replacement, and retryable
disk-overlap admission only while the exact checkpoint remains parked. Native,
complete quota-overlap, and loaded qualification remains pending; these results do
not establish a production latency or density improvement.

## Recovery boundary correction after rc19

A targeted failure experiment found a recoverable scheduling gap. A continuation
can queue while a RAM-backed capture holds the owner lock. If capture fails after
publishing its complete checkpoint, reconciliation finishes PARKED authority but
previously left the cached placement as RAM. The already queued continuation kept
requesting tmpfs until its admission deadline even when physical headroom was
available for file restore. This was reproduced with the real Warden, allocation
journal, registry and service; calling the existing fenced placement operation at
recovery changed the same case from admission timeout to success.

The existing placement operation now runs after successful recovered/imported
PARKED boundaries and after restore rollback commits PARKED. Running or uncertain
authority is unchanged, as is feature-disabled behavior. The queued-progress,
adoption, already-parked and absent-candidate cases are covered alongside existing
lifecycle/retention/growth tests: **127 Linux tests passed in 15.251 seconds**
([raw log](../benchmarks/resident-demand-2026-09-23/recovered-placement-linux-tests.log)).
This change is outside the frozen rc19 deployment and requires a later release.

## Deferred physical retention cleanup after rc19

Forced-load traces measured 58–309 ms in restore artifact cleanup. The reflink
path now retires checkpoint files during the existing Warden handoff, then leaves
physical trim and project-limit release to the service's existing reconciliation
loop. The direct registry's overlap claims are the durable worklist; there is no
new worker thread or independent cleanup queue. Non-reflink behavior is unchanged.

Maintenance marks the exact existing retention row `retiring` after proving the
generation directory absent and acquiring the exclusive allocation-reader
barrier. It drops owner locks before one physical trim for the collected projects.
Afterward it reacquires short barriers, checks absence and the original project,
digest and inode identity, commits local `deleted`, and only then releases the
matching global claim. Failure retains the claim for replay. Concurrent deletion
may finish an old project, but an old batch cannot retire a same-generation,
same-digest reimport with a fresh project. Deletion still completes its physical
barrier synchronously before committing the global owner deletion.

`sandbox.restore.artifact_unlink` and `sandbox.memory.retire_physical` spans separate
the two costs; the latter reports candidate and released counts. **107 Linux tests
passed in 10.744 seconds**, including one batch for eight generations, lock-free
physical work, existing maintenance integration, failed trim, crash after local
commit, retained readers, reappearing sources, and concurrent delete/reimport
([raw log](../benchmarks/resident-demand-2026-09-23/deferred-physical-linux-tests.log)).
This establishes the cleanup ordering, not a measured production latency gain.

## Action-aware selection after rc20

The rc19 pressure logs contain 113 completed native captures, 104 before the
failure timestamp. Of the 113, 105 were first captures and eight were subsequent
captures. Worker 12400716 recorded 24 first captures and no repeat capture; its
three cache probes reclaimed only 253,952 bytes. Other workers recorded five,
ten, and seven probes reclaiming 3.85, 5.40, and 6.21 GiB. These totals do not
explain each probe's result: the retained evidence lacks per-attempt cancellation
and kernel-reclaim reasons. They do not support claiming that worker 716 repeatedly
parked file-backed owners.

The policy nevertheless had a concrete action-selection bias. Owners with a
measured expensive park/wake history could rank below never-parked RAM owners,
even when their measured file-backed heap could satisfy the physical deficit
without a checkpoint. The rc21 change ranks that eligible retained-file action
first within the existing response-ready ordering. It uses the same fresh,
post-safe-wait cgroup measurements; it does not infer reclaimable bytes from a
configured limit. Tmpfs deficits and PSI-only probes retain their existing
selection. Unknown samples, in-flight byte credit, generation/wake fences, and
one cache probe per wait remain enforced.

A skipped or failed cache action must reselect and reserve its complete measured
checkpoint footprint before capture. This also covers active-exec preconditions
that prevent even starting cache reclaim; a small cache reservation cannot admit
a larger checkpoint. The existing telemetry now records one reclaim span with
application-file mode, target/requested/achieved bytes, and the bounded result
reason. There is no new controller, authority, or heartbeat schema.

The focused Linux gate passed **130 tests in 8.747 seconds**, including a 32-owner
small-cache/full-capture fallback burst, busy preconditions, response-ready
priority, tmpfs exclusion, stale/unknown measurements, cancellation and growth
admission. See [the raw log](../benchmarks/resident-demand-2026-09-23/action-ranking-linux-tests.log).
Independent review found and closed the fallback-credit gap before this gate.
This source change is outside the frozen rc20 release; production benefit is
pending a matching pressure run and is not claimed here.
