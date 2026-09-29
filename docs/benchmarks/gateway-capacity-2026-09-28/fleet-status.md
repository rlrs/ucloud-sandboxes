# Opt-in compact inventory projection

The gateway now supports `GET /v1/sandboxes?view=status`, optionally with repeated
exact `id` filters. [The API contract](../../fleet-status.md) describes fields,
limits and freshness. The default full response is unchanged. Existing clients
must opt in before these savings apply.

The status query omits full user specifications, resources and attached snapshot
descriptors before transferring database rows into Python. It retains lifecycle
identity, generation, timestamps and detached portability proof. The renderer
uses the existing visible-state calculation and complete worker heartbeats.
Each request reads fresh durable rows; caches reuse only unchanged encodings.
Full/status readers and differently filtered responses remain separate.

`scripts/benchmark_fleet_status.py` creates 500 or 1000 synthetic routes across
two workers, complete per-worker inventories, opaque layer descriptors and
representative managed-agent specifications. Each variant runs three alternating
samples of ten reads with persistent renderers. It asserts equivalent visible
status before measuring. Process CPU includes database reads, heartbeat decoding
and JSON rendering; excludes setup, heartbeat writes, IPC, HTTP, TLS, PostgreSQL
server CPU and client parsing. Python 3.10.13, local PostgreSQL 17 and SQLite.
These synthetic 1000-row results are not a qualification of 1000 live agents or
of fitting that workload's memory on two workers.

Median process CPU per request, milliseconds:

| Backend/routes | Unchanged full | Unchanged status | Reduction | Fresh heartbeat full | Fresh heartbeat status | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite / 500 | 6.844 | 4.319 | 36.9% | 37.058 | 32.574 | 12.1% |
| SQLite / 1000 | 14.896 | 9.393 | 36.9% | 70.029 | 60.316 | 13.9% |
| PostgreSQL / 500 | 4.633 | 3.690 | 20.4% | 21.751 | 18.642 | 14.3% |
| PostgreSQL / 1000 | 8.727 | 7.175 | 17.8% | 49.312 | 40.371 | 18.1% |

The fresh-heartbeat variant updates every worker heartbeat before each request,
forcing heartbeat revalidation and response encoding. It is an intentionally
different workload from unchanged reads. Production falls between these patterns
depending on heartbeat frequency and how HTTP workers receive polling traffic.

| Routes | Full response bytes | Status bytes | Reduction | Status for 16 exact IDs |
| --- | ---: | ---: | ---: | ---: |
| 500 | 659,054 | 168,071 | 74.5% | 5,447 |
| 1000 | 1,318,054 | 336,071 | 74.5% | 5,447 |

The filtered 16-ID unchanged read took 1.956/3.910 ms on SQLite and 2.567/4.832 ms
on PostgreSQL for 500/1000 total routes. It still reads complete heartbeats for
freshness and absence proofs; filtering does not make all work independent of
fleet size. The unfiltered status projection still scans all routes.

These are endpoint savings, not a whole-host CPU reduction. At twenty unchanged
500-route PostgreSQL polls per second, measured render/read savings amount to
about 0.019 process-core. The 74.5% byte reduction additionally avoids transfer,
TLS, IPC and client parsing work, which this benchmark does not quantify. The
earlier measured whole gateway API CPU bucket includes many other request paths.

Validation passed: 41 focused reader/cache/compact-API tests including four
PostgreSQL lifecycle/filter contracts, plus 83 existing control-plane tests.
Tests cover exact parameterized filtering, default full compatibility,
overlapping-query isolation and coalescing, error cleanup, deletion/generation
reuse, inventory absence, quarantine, clock expiry, detached portability proof,
corrupt lifecycle rejection, child restart and pre-IPC size validation.

Artifacts: [SQLite 500](fleet-status-500-sqlite.json),
[SQLite 1000](fleet-status-1000-sqlite.json),
[PostgreSQL 500](fleet-status-500-postgres.json),
[PostgreSQL 1000](fleet-status-1000-postgres.json).
