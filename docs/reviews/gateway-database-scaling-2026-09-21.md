# Gateway database scaling assessment

This is the initial options assessment. The subsequent
[shared control-plane design](../shared-control-plane-design.md) recommends a
PostgreSQL vertical slice for shared authority, without Redis, with explicit
qualification before migration. No migration has been implemented.

The gateway's SQLite write path is a measured latency bottleneck in the
[managed-agent reproduction](../benchmarks/relay-load-2026-09-21/README.md).
This does not establish a universal SQLite throughput limit or prove that a
plain database replacement will meet the subsecond wake target.

After removing repeated registry scans and reducing gateway inventory work,
a 40-sample profile found the placement-lock owner in `BEGIN IMMEDIATE` for
19 samples and commit for five. Selected requests waited over six seconds for
placement while their worker restore took roughly 0.6 seconds. These are stack
samples and selected traces, not estimates that SQLite consumes exactly 60%
of every request. The gateway also has only two vCPUs, and its process approaches
one full CPU core under the test.

A small mutex around local write transactions tests whether SQLite busy-handler
backoff is worsening contention. It does not increase write parallelism, replace
cross-process transaction fences, or coordinate writers in other processes.
Connection reuse was tested and rejected because the controlled pair did not
improve loaded tail latency. We should not build a bespoke database scheduler or
sharding system merely to preserve SQLite in the shared control plane.

SQLite permits one writer per database file, including in WAL mode. Its own
selection guidance recommends considering a client/server engine when many
concurrent writes or multiple application servers are required.
Sources: [SQLite workload guidance](https://www.sqlite.org/whentouse.html),
[WAL concurrency](https://www.sqlite.org/wal.html).

## Options before choosing a database

At the time of this initial assessment no migration was selected. Contention in this implementation is evidence
to investigate, not sufficient evidence that SQLite has exhausted its useful
capacity. We have not yet measured transaction arrival rate, writer utilization,
lock-hold distributions, or fsync/checkpoint cost sufficiently to distinguish a
fundamental single-writer limit from excess work inside our transactions.

| Option | What it addresses | Cost and decision criterion |
| --- | --- | --- |
| Keep SQLite, simplify the current transaction path | Redundant writes, large payload updates, repeated inventories and application locks held while waiting for the database | First choice to measure. These changes remain useful with any engine. Keep this approach if short durable transactions meet the target with measured headroom. |
| Explicit single-writer ownership or batching | Competition between local writers and repeated commits | A small transaction boundary can be reasonable. A new writer service, priority queues, cancellation protocol or crash-replay machinery would be substantial architecture; do not build these just to preserve SQLite. Batching must not acknowledge ownership before durability. |
| Partition state among embedded databases | Contention among independent ownership domains | Adds cross-partition reservation, migration and recovery coordination. Not a default next step: our capacity and sandbox transitions cross those domains. |
| Move shared state to PostgreSQL | Concurrent transactions and a database shared by multiple gateway processes/hosts | Requires service operation, backups, migrations, connection management and failure handling. Benchmark representative transactions first; row locks and application locks can still serialize the workload. |
| Replace SQLite with another embedded store or a custom journal | Potentially a better fit for a narrowly defined local access pattern | Requires evaluating indexes, atomic multi-record updates, durability, recovery, tooling and bindings. No evidence currently justifies this additional rewrite. |

The next experiment should time application-lock wait, SQLite writer acquisition,
transaction body and commit separately, tagged by operation. Count transactions
and bytes changed per park/wake, and correlate commit tails with disk and WAL
checkpoint activity. Repeat 64/256/512-agent tests with fixed worker placement and
matching candidates. Do not weaken durability to obtain a better latency number.
Only then compare a small representative PostgreSQL prototype if serialization
still dominates, or if multiple gateway hosts become a requirement.

## Why local persistence is a separate decision

A worker needs durable local process/device ownership to reconcile its resources
after restart, including when the gateway is unreachable. This requires local
durable state, not necessarily SQLite. The current registry already provides
transactions and recovery without another service, so retaining it is the lower
change-cost option until its own profile argues otherwise. That is independent
of whether the shared control plane outgrows its database.

Moving every record to central PostgreSQL would make recovery depend on network
and database availability unless we retain a local journal or explicitly change
the recovery contract. Running PostgreSQL on every worker would preserve locality
but add service operation on every ephemeral machine. Replacing local SQLite with
files or an append-only log would require implementing the atomicity, indexing and
recovery it currently supplies. None is automatically simpler merely because it
reduces the number of database engine names in the design.

## If shared-state migration proves worthwhile

The candidate scope is gateway routing, program requests, capacity reservations
and migration ownership, with relay state evaluated separately. PostgreSQL's
[concurrency model](https://www.postgresql.org/docs/current/mvcc-intro.html)
does not impose SQLite's database-wide single-writer restriction. Conflicting
updates still need deliberate transaction boundaries and locking.

A migration should change the hot transaction model as well as the driver:

- Reserve capacity on the target node and conditionally transition the exact
  sandbox generation/owner in one short transaction. Serialize conflicting
  reservations on that node, not every independent node in the fleet.
- Keep large sandbox specifications and checkpoint descriptors outside frequent
  state-only updates. Stop rebuilding an entire owner inventory for every wake.
- Preserve request idempotency, generation/operation fences, reboot epochs,
  deletion tombstones and migration recovery. Do not hold database locks across
  worker RPCs, publication, restoration or other external I/O.
- Keep diagnostic planning and bulk telemetry out of admission's critical
  transaction. A database change will not remove Python decoding work, global
  application locks or storage synchronization elsewhere in the pipeline.
- Qualify concurrent gateway processes, stale heartbeats, conflicting creates,
  lost acknowledgments, retries and worker loss with the same loaded harness.
  Measure transaction wait, commit time, admission time and end-to-end tails at
  64, 256 and 512 agents, including restart/recovery correctness.

These are options for assessment, not an approved migration. No PostgreSQL service, new runtime
dependency, or production data migration was introduced in this investigation.
