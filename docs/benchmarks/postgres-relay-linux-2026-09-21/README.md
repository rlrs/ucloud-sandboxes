# PostgreSQL relay: Linux integration and failure qualification

21 September 2026. This tests the actual selectable relay backend and HTTP app,
not just the standalone scheduling prototype. Production has not been migrated.

## Results

All eight final trials delivered and checked every distinct response: **4,096
responses**, no HTTP errors, no remaining inference or pending delivery. Three
normal synchronized bursts had p95 **0.906–1.148 s**; five trials that killed one
relay had p95 **1.702–2.076 s**. Broken connections were retried only in kill trials.
An HTTP 500 fails either scenario, including the worker acknowledgment path.

These times include a **simulated 600 ms wake callback**. They do not establish
subsecond product wakes or 512 real-sandbox capacity. There is no real gateway,
gVisor checkpoint/restore, storage contention, model inference or tool execution
in this test. Two relays and the database share one development host. The full
gateway/scheduler/provider authority migration remains outstanding.

| Trial | Responses | p50, s | p95, s | p99, s | Max, s |
| --- | ---: | ---: | ---: | ---: | ---: |
| [ucloud-relay-final-burst-1](ucloud-relay-final-burst-1.json) | 512/512 | 1.132 | 1.148 | 1.148 | 1.150 |
| [ucloud-relay-final-burst-2](ucloud-relay-final-burst-2.json) | 512/512 | 1.016 | 1.108 | 1.109 | 1.112 |
| [ucloud-relay-final-burst-3](ucloud-relay-final-burst-3.json) | 512/512 | 0.898 | 0.906 | 0.907 | 0.909 |
| [ucloud-relay-final-kill-1](ucloud-relay-final-kill-1.json) | 512/512 | 1.187 | 1.753 | 1.757 | 1.757 |
| [ucloud-relay-final-kill-2](ucloud-relay-final-kill-2.json) | 512/512 | 1.168 | 1.743 | 1.747 | 1.747 |
| [ucloud-relay-final-kill-3](ucloud-relay-final-kill-3.json) | 512/512 | 1.429 | 1.817 | 1.820 | 1.821 |
| [ucloud-relay-final-kill-4](ucloud-relay-final-kill-4.json) | 512/512 | 1.571 | 2.076 | 2.077 | 2.079 |
| [ucloud-relay-final-kill-5](ucloud-relay-final-kill-5.json) | 512/512 | 1.183 | 1.702 | 1.913 | 1.913 |

## Environment and method

- Host `rasmus-dev`: Linux 5.15.0-190-generic, KVM VM exposing 30 vCPUs
  (AMD Ryzen Threadripper 7960X), approximately 80 GiB RAM.
- Disk exposed as a nonrotating 1,000 GiB QEMU block device. These are guest
  observations, not claims about host storage latency or durability.
- Managed Linux Python 3.13.12; PostgreSQL 17.11 in an isolated Docker network.
  No database host port. `fsync`, `full_page_writes` and `synchronous_commit` on;
  100 PostgreSQL connections allowed, 16 pooled connections per relay plus LISTEN.
- Two independent relay processes, real HTTP requests and real PostgreSQL
  transactions. The test driver raises its descriptor allowance to 8,192.
- 512 agents register and enqueue, inference workers poll, then all 512 distinct
  32 KiB response bodies become ready at once. Bodies are synthetic and
  compressible. Timing starts before response submission; there is no forced
  wait for parking. Every response is checked against its own expected bytes.
- Kill trials send SIGKILL to relay 0 after 100 ms of the response burst. The test
  uses a one-second dispatcher claim lease for recovery, versus the production
  default of 30 seconds. Normal trials use the production default.
- Trials ran sequentially without another qualification test running. No
  artificial CPU or I/O isolation from other host activity was applied.
- JSON files record source SHA-256 hashes, timestamps, latency distributions,
  connection retry counts, durable state and transaction timing. Transaction
  metrics include setup/parking as well as response delivery; they are not an
  isolated breakdown of the response burst or pure database lock-wait metrics.

## Correctness and packaging checks

- Full Linux server suite: **1,117 tests, six skipped**, 118.851 seconds, passed.
  The PostgreSQL tests were enabled against the real database.
- Focused contracts: **48 tests passed**, comprising 24 live relay tests and
  24 standalone scheduling-contract tests.
- Committed results, wake work and reservation survived SIGKILL/restart of the
  isolated PostgreSQL container. Verification completed and removed its schema.
  This checks database-process failure with intact storage, not standby failover
  or host/disk loss.
- Linux wheel build and clean default wheel installation passed the packaged
  schema/import smoke check. Local Ruff and shell syntax checks also passed.
- No production deployment, production PostgreSQL cutover or SDK change was made.

During development, a kill trial exposed a real reattach race: separate READ
COMMITTED reads could see a leased request followed by its peer's deletion of
the request payload. A deterministic real-database test reproduced that HTTP 500.
The read now joins state and bodies in one statement snapshot. Earlier successful
trials are not substituted for the final eight recorded here.

Other measured-path changes move batched NOTIFY hints after durable commits,
batch socket readiness reads, hydrate dispatch metadata during claiming, and
avoid fetching response bodies for worker acknowledgments. A lost hint still
recovers through durable reads. Slow wake acknowledgment explicitly preserves the
committed result for retry.

## Reproduction and remaining acceptance

See [backend setup and cutover](../../postgres-relay.md) for the checked-in Linux
Compose commands, configuration and migration safeguards. CI runs real PostgreSQL
contracts, a 64-request multiprocess kill test and the database-crash check.

Before production migration, qualify the PostgreSQL hosting/failover policy,
rehearse cutover against a stopped production-format relay journal, and run the
[live workload](../../relay-load-benchmark.md) with real workers at 64/256/512.
Gateway ownership/placement is still SQLite and must remain one authority. These
results do not authorize running several independent gateway controllers.
