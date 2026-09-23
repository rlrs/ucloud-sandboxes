# Managed primary-process growth admission

A completed sandbox create does not mean its workload has allocated memory. In the
rc12 pressure trial, 256 managed primaries were asked to allocate 1.5 GiB each on
four workers with approximately 87.8 GiB of physical memory each. This exceeds
physical capacity before runtime overhead. Releasing the startup reservation as
soon as the empty runtime starts leaves the subsequent allocation burst invisible
to admission. The trial recorded 16 guest SIGBUS exits without a kernel OOM; it did
not retain a tmpfs free-space sample at the faults, so the exact fault cause remains
unproven.

The worker now retains a **growth forecast** for its managed primary, using the
existing DirectRegistry and TransitionLedger. This is not a new scheduler or a
permission to run a workload. The supervisor already permits exactly one primary
job and command specification for a sandbox generation, including after the job
terminates. The worker records that same immutable launch identity.

- A queued launch cannot dispatch its control RPC until the existing startup queue
  and live resource checks admit it. The active forecast is committed before the
  RPC. An ambiguous response retains it across daemon restart.
- Active residual growth is the configured memory bound minus a fresh observed
  cgroup footprint. RAM-backed workers credit only the shared-memory portion that
  is charged to both physical memory and tmpfs. Missing or stale observations give
  no credit. TransitionLedger combines overlapping operations by incarnation, so
  the same primary is not charged twice.
- An authenticated, generation-bound relay model wait suspends the future-growth
  forecast. The running guest's actual allocation is already reflected in host
  memory and backing-space measurements. Advisory phase messages do not release
  the forecast.
- A relay continuation reacquires its growth forecast before the worker acknowledges
  the wake. While it queues, its current model wait remains eligible for ordinary
  pressure parking. The grant and that request's durable wake fence commit together;
  an old park cannot subsequently erase the grant. This ordering avoids removing
  every reclaim candidate during a simultaneous response burst.
- A committed checkpoint suspends growth. Managed restore reserves the larger of
  authenticated checkpoint allocation and the primary's memory bound, then restores
  the active forecast. The forecast survives ordinary tool-triggered restores too.
- Matching terminal observations and completed deletion retire the forecast.
  Different jobs or generations cannot retire or overwrite an ambiguous launch.
  Imported primary identity remains unknown until a successful supervisor response
  binds it; cold restore still uses the conservative memory bound.

Admission compares pending known bytes with both physical memory headroom and the
verified RAM-backing filesystem's available bytes. Free swap does not satisfy a
`noswap` tmpfs allocation. An absent configured backing mount or unreadable evidence
queues admission with the existing deadline; it is never interpreted as unlimited
space. The policy uses the same backing evidence when deciding whether resident
waits need parking. Clean filesystem-cache reclaim cannot solve a tmpfs-space
shortfall.

The existing worker SQLite ownership schema advances from v4 to v5; v3 and v4 have
bounded forward migrations. It does not change gateway PostgreSQL ownership or the
portable checkpoint format. Rollback to a worker binary that only reads v4 requires
draining/replacing that worker, rather than opening its v5 journal with the old
binary.

This is a forecast, not a hard guarantee against an arbitrary guest allocating
while supposedly waiting for a model. Its observation cache is bounded by the
existing resident sampler. Exact guarantees against that behavior would require
keeping full memory quotas reserved or runtime allocation backpressure. We have not
introduced either silently.

Validation before rollout: fourteen assembled managed-service tests cover simultaneous
launches, residual/stale footprint, ambiguous RPC and restart, immutable primary
identity, matching terminal/delete, queued deletion/drain, already-running wake,
parking during continuation admission, atomic late-wait fencing, forced capture and
restore, imported terminal-primary cleanup, crossed capture/continuation with a real sparse
manifest, safe retry before dispatch/continuation acknowledgement, preservation of
ambiguous restore errors, and v4 migration. Existing registry/transition tests also pass. These
checks establish admission and ownership behavior; the actual 256-agent pressure
trial and its backing-capacity time series remain the required performance and
failure-reproduction gates.

The first rc15 pressure rerun did not complete: one queued managed launch exceeded
its request deadline and its untyped internal HTTP 503 escaped to the runner. The
worker now classifies only that pre-dispatch growth deadline, and the analogous
pre-acknowledgement continuation deadline, with the existing retryable activation
response. Supervisor and native-restore failures remain outside that boundary.
See the [retained trial evidence](benchmarks/architecture-load-2026-09-23/rc15-pressure-memory/README.md);
a sustained successful rerun remains required.
