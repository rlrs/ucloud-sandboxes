# Shared-control qualification primitives

The scheduling qualification slice of the
[shared control-plane design](shared-control-plane-design.md) is available in
`ucloud_sandboxes/shared_control`. It is an executable qualification backend,
**not a selectable production replacement for gateway scheduling**.
The live relay backend is now integrated separately; see [PostgreSQL relay](postgres-relay.md)
for configuration, fenced worker dispatch, idle cutover and Linux HTTP qualification.
The fixture-based scheduling slice described below is still not a gateway replacement.
It lives in `shared_control/qualification.py` as `QualificationControlStore`.
Both stores share only `PostgresDatabase` connection/transaction facilities;
production relay migration creates no fixture scheduling tables. The qualification
commands below explicitly initialize and report the separate experiment schema.

## Implemented

- An explicit versioned PostgreSQL schema and async connection pool. No
  process-wide writer/placement mutex, automatic startup DDL, or Redis dependency.
- Atomic model-response persistence and wake intent, including status, headers,
  body and immutable replay identity. A duplicate result cannot allocate another
  wake or overwrite a different response.
- Short per-sandbox result transactions. These do not lock the node: physical
  reservation is a separate dispatch transaction scoped to the affected node.
- Durable `SKIP LOCKED` operation claiming, claim tokens, retry scheduling and
  per-node temporary restore reservations. Expired claims and uncertain worker
  outcomes retain reservations and retry the same operation.
- Conditional completion using sandbox generation, create identity, spec hash,
  node boot epoch, lifecycle sequence, claim token and newer activity proof.
- An async dispatcher that holds no database connection during worker calls and
  refills individual execution slots. A slow RPC does not stall an entire batch.
- Pool wait, transaction, commit and locking-query timing. Lock-query time
  includes execution/network time; it is not a pure PostgreSQL lock-wait metric.
- Real PostgreSQL integration tests, a two-phase database crash test, CI coverage,
  and a repeatable PostgreSQL coordination benchmark.

The optional `postgres` extra keeps database libraries off existing worker and
worker deployments; the live relay requires it. The SQL schema is shipped in the wheel.

## Run the contract

Use an isolated PostgreSQL 17 test database. Tests create uniquely named schemas
and remove only those schemas afterward. The benchmark also uses a fresh schema
without touching existing production routes.

```sh
uv sync --locked --extra postgres
UCLOUD_TEST_POSTGRES_DSN='host=/private/test/socket dbname=postgres' \
  uv run --extra postgres python -m unittest tests.test_shared_control_postgres
```

Tests cover concurrent duplicate/conflicting results, coalescing, lease expiry,
transaction rollback, unrelated-node progress, node capacity deferral, claim
expiry, stale proofs, deployment isolation, cancellation, abrupt dispatcher
process exit and lost acknowledgments. CI starts PostgreSQL and additionally
kills/restarts the database between the crash test's two phases.

Explicit schema commands take a private DSN file rather than a credential-bearing
command argument. They do not modify `DeploymentConfig` or select a live backend:

```sh
uv run --extra postgres python -m ucloud_sandboxes.shared_control qualification-migrate \
  --dsn-file /private/path/database-dsn --deployment-id qualification
uv run --extra postgres python -m ucloud_sandboxes.shared_control qualification-status \
  --dsn-file /private/path/database-dsn --deployment-id qualification
```

## Measure coordination

```sh
uv run --extra postgres python scripts/benchmark_shared_control.py \
  --dsn-file /private/path/database-dsn --agents 512 --nodes 4 \
  --connections 16 --dispatchers 2 --repeats 3 \
  --output /private/path/burst-512.json

uv run --extra postgres python scripts/benchmark_shared_control.py \
  --dsn-file /private/path/database-dsn --agents 512 --nodes 4 \
  --arrival-rate 41 --restore-ms 600 --repeats 2 \
  --output /private/path/steady-512.json
```

The benchmark repeats PostgreSQL trials, preserves durable commits, records
source hashes and excludes fixture setup. Burst mode offers all results together.
Steady mode schedules arrivals independently of processing; driver lateness counts
against latency. The worker callback is simulated. `--restore-ms` is a timer,
not a measurement of runsc, memory restoration or disk throughput.

The historical SQLite comparison is archived in earlier benchmark reports.
The current benchmark exercises only PostgreSQL coordination and simulated
worker callbacks. Independent pools/dispatchers share one Python process; this
does not qualify multi-host gateway operation. Read its limitations before
attributing changes to storage or runtime performance.

The live SDK harness now defaults to natural model readiness and reports
`response_ready_to_usable_exec_seconds` as its acceptance metric. Forced-park mode
remains available, but its extra parking wait is included in that metric.
See [live load instructions](relay-load-benchmark.md).

## Boundary before production integration

`fixtures.py` imports a parked inventory and already-leased model requests solely
for qualification. It is not the public create, registration or lease API.
The worker callback supplies a fenced proof; the deployed worker HTTP protocol
has **not** gained lifecycle sequencing merely because this database stores it.

The remaining production work is explicit: authoritative registrations and lease
acquisition/renewal, create/delete/generation allocation, inventory reconciliation,
drain and migrations, node-loss handling, provider operation migration, actual
worker sequencing, response delivery/retention and HTTP integration. Restore
budgets here are fixture inputs; adaptive pressure admission is not implemented.
There is no automatic claim that a timeout means a worker is dead, and no VM
termination path in this slice.

Do not point the prototype at production or bridge two independently authoritative
SQLite/PostgreSQL schedulers. Qualify those state transitions and the worker
protocol before implementing the design's single-authority cutover. A successful
coordination benchmark does not establish the subsecond product target.
