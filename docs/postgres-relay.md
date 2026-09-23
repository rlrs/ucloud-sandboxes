# PostgreSQL relay backend

As of **0.5.114**, PostgreSQL is the only live model-relay authority for
registrations, inference leases, retry identity, responses and park/wake work.
Configure `relay_postgres` before starting the relay. Old live SQLite
configuration fails with migration instructions; it never silently starts a
fresh authority. Gateway ownership, placement, provider and worker-local
journals remain separate authorities; keep one gateway/controller authority.

A worker response commits once with its wake intent, then immediately returns
HTTP 200 with a durable-acceptance receipt:

```json
{"ok": true, "request_id": "...", "duplicate": false, "committed": true, "delivery_status": "pending"}
```

`pending` means the committed delivery obligation is still outstanding.
`released` means the response is eligible for sandbox-facing delivery (or its
caller was definitively lost), **not** that the sandbox received it or is awake.
The value is a snapshot at commit/retry time, not a delivery subscription. A
successful submission releases inference capacity; callers that require useful
continuation must observe the sandbox's own response receipt and continuation.

If the HTTP connection disappears after commit, background dispatch keeps
retrying wake. Another relay process accepts an identical authenticated retry
without sampling again; it returns `duplicate: true` and the current delivery
status. Changed content or a different inference lease is rejected. Wake delay
no longer turns an accepted response into HTTP 504. Terminal `/worker/error`
responses use this receipt too; retryable inference errors still return
`retried: true` because they requeue inference rather than commit a result.

The SDK accepts both this receipt and older servers' legacy acknowledgments.
Existing integrations must stop treating completion success as proof of wake.

### SQLite migration boundary

0.5.114 removes the live SQLite backend and its synchronous wake-before-ack
notifier. The fenced, offline, one-way importer below retains version-3 row
compatibility. Stop the old relay and complete that cutover before upgrading an
unmigrated deployment. Do not delete its journal or remove its authority fence.
The importer bounds input to 100,000 rows and 512 MiB of encoded row payloads;
larger journals require an explicit operator migration rather than unbounded
startup decoding. There is no dual write or automatic backend selection.
Gateway and worker-local SQLite journals are not deprecated by this boundary.

Production `live-ucloud-20260824a` enabled this backend on 2026-09-22. See the
[cutover and recovery record](benchmarks/postgres-production-2026-09-22/README.md).

## Runtime behavior

- Independent relay processes share registrations, queues and inference leases.
  `FOR UPDATE SKIP LOCKED` claims work; lease tokens reject stale workers.
- Result bytes and wake intent commit atomically. A changed duplicate result is
  rejected; an identical committed response remains replayable after lease expiry.
- Dispatchers hold no database transaction across a gateway RPC. Restore pressure
  delays queued work; it does not turn a committed response into a client error.
- Wake can overtake a delayed park acknowledgment. Gateway checks and a durable
  worker fence prevent that request's late park from undoing its wake. Workers
  must advertise `relay-wake-fence-v1`; the gateway refuses an unsafe fallback.
- Model bodies live separately from mutable state. Lease renewals do not rewrite
  payloads. HTTP waiters share batched delivery reads rather than each polling a
  database connection. Worker acknowledgments are constructed from the commit
  result without readiness polling or another response-body read. Socket futures
  and notification hints are disposable.
- Notifications are batched **after** the durable commit. PostgreSQL serializes
  notifying writers until commit; putting every notification in the result
  transaction defeated concurrent WAL flushing in the Linux load test. Dropped
  hints are recovered by periodic durable reads. See the PostgreSQL
  [notification implementation](https://doxygen.postgresql.org/async_8c_source.html).
- A configured storage budget reserves the maximum response space before accepting
  a request. Completed responses release unused reservations in background batches.
  This is a disk safety boundary, separate from execution concurrency. Results
  waiting for wake are pinned and never evicted to make room for new work.
- Request expiry creates a terminal response and queues its caller's wake.
  Definitive caller loss releases delivery while preserving a committed result.

`/v1/relay/stats` reports pending/leased requests, pinned delivery age, lifecycle
queue/retries, reserved storage and database-pool statistics. The existing
telemetry exporter records `ucloud.platform.postgres.duration`, with operation,
phase (`pool_wait`, `transaction`, `commit`, `lock_query`) and status labels.
There are no request-ID metric labels.

## Configuration and deployment

Install the server wheel with the `postgres` extra. Both UCloud and Hetzner
installation paths select this extra when `relay_postgres` is configured. Worker
bundles continue without database drivers.

```json
"relay_postgres": {
  "dsn_file": "/etc/ucloud-sandboxes/postgres-dsn",
  "schema": "ucloud_shared",
  "max_connections": 16,
  "storage_budget_bytes": 68719476736
}
```

The DSN file must be a private regular file, readable by the relay service user
and mode `0600`. Keep credentials out of deployment JSON, shell arguments and
logs. Configure the DSN for the database's actual TLS/authentication policy.
The schema name is restricted to `ucloud_shared` or an isolated
`ucloud_shared_<suffix>` namespace. Each relay also opens one LISTEN connection;
include that in PostgreSQL's connection budget.

The implementation uses synchronous commits. This protects acknowledged results
from a database-process crash with intact storage. It does not itself provision
replication, backups or failover: use a PostgreSQL deployment with the durability
and recovery policy required for production. Never disable fsync or synchronous
commit to reproduce a latency figure.

## Database boundary

`PostgresDatabase` owns connections, transaction metrics, commit hints and relay
schema migration. `PostgresRelayState` owns the live domain. New `migrate` calls
create only `relay_*` tables, and `status` reports the relay without starting a
dispatcher or writing runtime configuration. These commands no longer initialize
or report the unshipped central-scheduling experiment.

Existing qualification tables are left intact; their users must be checked
before any manual cleanup. The explicitly named `QualificationControlStore`
and `qualification-migrate` / `qualification-status` commands remain available
for isolated scheduling experiments. They never replace gateway authority.

## Idle cutover

1. Upgrade gateway and worker code first; verify live workers advertise
   `relay-wake-fence-v1`. The worker's version-3 local registry is transactionally
   upgraded to version 4, preserving registrations. Keep the upgraded worker code
   on rollback: older worker binaries cannot read this journal.
2. Initialize the PostgreSQL schema explicitly with the migration command below.
   Runtime startup performs no DDL and rejects an unknown or missing schema.
3. Wait for relay `inflight=0` and `delivery_pending=0`, then stop **all** relay
   processes. Keep the gateway/controller and workers running. Back up the stopped
   SQLite relay database, including its retained responses and tokens.
4. Import the stopped relay using `import-idle-relay`. The importer refuses active
   work and a nonempty target authority. It preserves registration tokens,
   idempotency identities and retained results.
5. Install the PostgreSQL extra, add `relay_postgres`, and start the relay. Validate
   health, registration, model requests, park/wake and result replay before load.
   Additional relay processes must use the same deployment/schema and lifecycle
   notifier configuration. Their gateway target must reach the same authoritative
   gateway; the current CLI uses the local gateway endpoint.

```sh
python -m ucloud_sandboxes.shared_control migrate \
  --dsn-file /etc/ucloud-sandboxes/postgres-dsn \
  --deployment-id YOUR_DEPLOYMENT

python -m ucloud_sandboxes.shared_control import-idle-relay \
  --dsn-file /etc/ucloud-sandboxes/postgres-dsn \
  --deployment-id YOUR_DEPLOYMENT \
  --sqlite-file /absolute/path/to/model-relay.sqlite3
```

The import first commits an inactive PostgreSQL copy. It then durably fences the
source journal and activates PostgreSQL. A crash between these commits fails
closed; rerun the command with the same unchanged source to finish activation.
The source gets both an authority marker and an unsupported SQLite journal
version, so earlier releases also refuse to reopen it. A running old process
must still be stopped before import; a journal version cannot revoke its memory.

Do not remove the fence or restart an old SQLite copy after PostgreSQL has served
traffic. That would resurrect stale registrations/results. There is no automatic
reverse migration or dual-write mode. An import whose source changed after the
provisional copy refuses to activate and requires an operator to reconcile it.

## Linux qualification

[Recorded Linux results](benchmarks/postgres-relay-linux-2026-09-21/README.md)
include repeated 512-request HTTP bursts, process failure and database recovery.

The checked-in Compose stack isolates PostgreSQL on an internal Docker network
with no published port. Its trust authentication is **test-only**. It uses uv's
managed Linux Python because the older SQLite library in Debian's system Python
cannot run the worker registry's existing `trusted_schema=OFF` JSON constraints.

```sh
docker compose -p ucloud-pg-qualification \
  -f tests/linux_shared_control/compose.yaml run --build --rm tests \
  python -m unittest tests.test_postgres_relay tests.test_shared_control_postgres

docker compose -p ucloud-pg-qualification \
  -f tests/linux_shared_control/compose.yaml run --rm tests \
  python scripts/benchmark_relay_http.py --agents 512

docker compose -p ucloud-pg-qualification \
  -f tests/linux_shared_control/compose.yaml run --rm tests \
  python scripts/benchmark_relay_http.py --agents 512 --kill-relay
```

The HTTP benchmark launches **two separate relay processes**, sends actual HTTP
requests through the production app, verifies every body, and checks durable work
drains. The kill option abruptly kills one process during the response burst and
reattaches clients to its peer. Each request has a distinct response body, and
HTTP errors fail the trial; only broken connections are retried in the kill test.
Wake duration is a **simulated timer**. It does not
measure gVisor restore, disk contention, actual gateway placement or usable exec;
use the [live workload](relay-load-benchmark.md) for that product acceptance test.

The database-crash script exercises both the standalone scheduling contract and
the live relay tables. Run its `prepare` phase, kill/restart only the test
PostgreSQL container, then run `verify` with the same `ucloud_shared_crash_*`
schema. CI performs the same check. Test schemas are removed after verification.
