# Keyed response delivery, real PostgreSQL

The relay already received request-specific response notifications, but each
notification scanned every local response waiter and active park operation.
At 512 pending callers, a sequence of 32 independent hints therefore decoded
16,384 status rows. The new in-memory dirty-ID set retains the existing hint
identity: ordinary hints query only affected watched requests, while a fixed
monotonic 0.5-second reconciliation still queries all watched requests. Ongoing
unrelated hints cannot postpone missed-notification recovery. This set has no
durable authority and does not change result bodies, authentication, dispatch,
fencing, or response acknowledgement semantics.

An isolated Linux Python 3.13/PostgreSQL fixture ran the old and new delivery
loops in ABBA order. Each trial created 512 real durable requests, attached 512
socket futures, and delivered 32 separated keyed hints. Setup and initial
all-waiter registration were excluded from measurement. No production database
or worker was changed. Both paths issued 32 status queries.

| Measurement | Baseline range | Dirty-ID range |
|---|---:|---:|
| Returned status rows | 16,384 | 32 |
| Python process CPU | 42.6–47.5ms | 11.0–12.1ms |
| Status transaction elapsed time, summed | 63.9–65.2ms | 9.3–10.0ms |
| Hint-loop elapsed time | 143.2–145.6ms | 83.8–84.9ms |

Python CPU excludes PostgreSQL server CPU. Transaction elapsed time includes
client decoding and I/O; it is not a database CPU measurement. These four small
trials establish removed amplification, not a fleet p95 or throughput claim.
Periodic full reconciliation remains necessary and is included in correctness
tests rather than these sub-0.5-second steady hint measurements.

All 55 real PostgreSQL relay tests passed, including the new scaled fetched-row
assertion and a completed response whose notification is lost while unrelated
hints arrive continuously. Existing tests cover restart, response retention,
lifecycle cancellation, independent delivery obligations and reattachment.

`results.json` retains exact values and source digests. `benchmark.py` is the
small qualification fixture; run it from the repository with the PostgreSQL test
dependencies and `UCLOUD_TEST_POSTGRES_DSN` pointing at an isolated test database.
It creates and removes unique qualification schemas. Place the frozen baseline
relay module at `/tmp/relay-before-dirty-delivery.py` first; only its delivery
method is used for the before comparison. Never point this fixture at production.
