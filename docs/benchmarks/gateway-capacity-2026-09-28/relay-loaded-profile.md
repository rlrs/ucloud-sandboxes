# Relay CPU with the admission budget corrected

The relay used **0.427 CPU cores on average** during a valid 30-second
profile with all 512 synthetic agents active. Its previous near-one-core
measurement occurred while an 8 GiB reservation budget was forcing retries;
it is not evidence that one Python relay process inherently saturates at
512 agents. This profile does not justify a Rust rewrite.

## Capture

- Run: `relay-load-e2388974345d`, eight host CPUs, corrected 64 GiB relay budget.
- All agents ready: **13:26:59.072370 UTC**.
- Profile exclusion window: **13:26:59.760490–13:27:31.306163 UTC**.
- Relay PID 46832, unchanged process start time throughout; 13.46 CPU seconds
  over 31.5456 elapsed seconds, including profiler startup/finish overhead.
- `py-spy record --gil --rate 99 --duration 30 --format raw` collected
  **1,019 samples with zero sampling errors**.
- A local guard checked the exact run's log every second. It required 512
  distinct started agents and `all_agents_ready`, a recently updated log,
  and no failed/completed scenario or finished benchmark before, during or
  after capture. Remote heartbeat loss also stopped capture. No profiler
  command was repeated during artifact retrieval.
- At 13:27:17, relay stats showed 481 in-flight requests, 16.675 GB of logical
  reservations, no database pool waiters and one pending delivery. At
  13:29:07 there were 493 in-flight requests and no pending deliveries; the
  main pool had accumulated only 157 ms of queue wait across 45,835 requests.
  These counters exclude a sustained pool-capacity bottleneck in that window.

The raw profile, timing/guard receipt, frame summary and five-second aggregate
stats are the adjacent `optimized8-512-relay-*` artifacts. Exclude the profile
window from unprofiled latency and host CPU comparisons. The completed run
finished all 512 agents and 8,192 cycles with correctness passing and no
cleanup errors. Its configured latency SLO did **not** pass; the unfiltered
post-provisioning useful-action p95 was 2.072 seconds against a 1-second target.
That statistic includes the designated profiler and mixed-traffic windows.
A successful profile or correctness result is not a latency qualification.

The full 512-agent plateau lasted 342.078 seconds, until the first scenario
completed at 13:32:41.150499 UTC. Across 68 five-second samples in that plateau,
in-flight requests ranged from 449 to 498 (median 478.5); reservations peaked
at 18,993,314,358 bytes, or 27.6% of the 64 GiB budget. No stats requests failed.
The main pool accumulated 1,852 ms of queue wait across 84,622 acquisitions
between the first and last plateau samples (0.022 ms per acquisition), with
no acquisition errors. The separate lifecycle pool accumulated 2,129 ms over
143,000 acquisitions (0.015 ms each), also without errors. Pending delivery
age peaked at 0.130 seconds among these samples. These are sampled diagnostic
counts, not worst-case guarantees between samples.

## Matched four-CPU capture

The same guard and profiler settings captured run `relay-load-e3c849dc313a`
immediately after all 512 agents were ready at 13:36:10.250689 UTC. Its profile
window was **13:36:13.173097–13:36:44.441596 UTC**, with 1,096 GIL samples and
zero sampling errors. All 512 agents remained active throughout. The relay
was the same PID 46832 and process incarnation; `/sys/devices/system/cpu/online`
subsequently confirmed CPUs `0-3` online during this run.

| Metric | Eight host CPUs | Four host CPUs |
| --- | ---: | ---: |
| Relay CPU seconds during capture | 13.46 | 13.54 |
| Capture elapsed seconds | 31.546 | 31.268 |
| Average relay CPU cores | 0.427 | 0.433 |
| GIL samples | 1,019 | 1,096 |
| psycopg exclusive samples | 36.1% | 40.4% |
| asyncio exclusive samples | 22.5% | 23.7% |
| BEGIN/COMMIT inclusive samples | 21.5% | 19.0% |

There is no material relay CPU increase in these captures. They measure the
relay process, not the entire host, and do not by themselves qualify the
four-CPU host's end-to-end latency. The completed four-CPU run finished all
512 agents and 8,192 cycles correctly, with no errors or cleanup errors. Its
configured latency SLO did not pass: the unfiltered post-provisioning useful-
action p95 was 2.295 seconds, including the designated profiler and mixed-
traffic windows. Its adjacent `optimized4-512-relay-*` artifacts contain the
raw profile, guard receipt, frame summary and five-second stats. Exclude both
profile windows from the normal capacity and latency comparison.

The four-CPU full-agent plateau lasted 349.167 seconds, ending at the first
scenario completion at 13:41:59.417932 UTC. Across 69 five-second samples,
in-flight requests ranged from 438 to 497 (median 481); peak reservations were
20,325,008,726 bytes (29.6% of budget). All 93 stats probes over the run
succeeded. The main pool accumulated 6,502 ms of wait over 84,512 acquisitions
between plateau samples (0.077 ms each); the lifecycle pool accumulated
5,976 ms over 132,368 acquisitions (0.045 ms each). Both had zero acquisition
errors. Pending delivery age peaked at 0.367 seconds among these samples.
The final acceptance run uses stats sampling only and has no py-spy window.
Reservation totals include earlier runs' retained responses, so the higher
four-CPU total does not establish a CPU-dependent storage increase.

The adjacent `optimized4-512-host-full-plateau.json` analyzes 349 one-second
host samples over the exact full-agent plateau. Every interval had four CPUs
online. Whole-host busy CPU averaged **1.972 cores**, with **2.770 cores p95**
and a **3.379-core maximum**. Component means were 0.686 for the gateway
service, 0.437 for the relay, 0.503 for PostgreSQL, 0.109 for the registry
container and 0.107 for nginx. These full-window numbers include both the
profiler and the earlier mixed network workload; the final acceptance uses
its separately identified network workload and clean timing window.

## Final acceptance, with no profiler

Run `relay-load-cac265f5811c` used four CPUs and the final gateway serializer
change. All 512 agents completed all 5,120 cycles correctly, with no workload
or cleanup errors. The 512-agent plateau ran from **13:47:29.894386 through
13:50:46.629393 UTC** (196.735 seconds); benchmark cleanup finished at
13:51:53.120376 UTC. No py-spy capture ran during this test.

The post-provisioning useful-action p95 improved to **1.034822 seconds**.
That still narrowly misses the benchmark's strict 1-second target; report
correctness and capacity separately from the unmet latency threshold.

All 61 five-second relay stats probes succeeded. Within the full-agent
plateau, 39 samples showed 460–497 in-flight requests (median 484), with a
peak reservation of **21,430,934,604 bytes**, or 31.2% of the 64 GiB budget.
The main pool's 50,230 acquisitions accumulated 1,565 ms of queue wait
(0.031 ms each); the lifecycle pool's 81,933 acquisitions accumulated
3,036 ms (0.037 ms each). Neither pool recorded acquisition errors. The
oldest pending delivery peaked at 0.225 seconds among these samples.
`acceptance4-512-relay-stats.jsonl` and its adjacent summary retain the
aggregate evidence. Earlier completed runs still contribute retained
response bytes to the reservation totals.

`acceptance4-512-host-full-plateau.json` contains 196 host samples, covering
195.046 seconds of this plateau. All intervals had four CPUs online. Whole-
host busy CPU averaged **2.280 cores**, with **3.100 cores p95** and a
**3.530-core maximum**. Relay CPU averaged 0.480 cores; PostgreSQL 0.558,
gateway 0.739, registry container 0.231 and nginx 0.114. Listen overflows and
drops stayed zero. I/O wait averaged 0.209 cores; CPU `some` pressure averaged
25.2% of elapsed time (p95 46.2%), so runnable contention was measurable even
though one-second aggregate CPU samples stayed below four busy cores. These
results support a measured capacity assessment, not a claim of zero
contention or passage of the 1-second latency target.

## CPU attribution

Exclusive GIL samples were 36.1% psycopg, 22.5% asyncio, 11.7% application
frames, 10.3% aiohttp, 3.1% psycopg pool, 2.75% JSON and 2.26% Base64; the
remaining 11.3% were other Python/runtime frames. This distribution points
to SQL protocol and asynchronous scheduling overhead rather than one large
JSON serialization hotspot. GIL samples describe Python-side work. An active
psycopg frame does not measure PostgreSQL server lock wait, query duration or
off-GIL/native CPU.

BEGIN/COMMIT round trips appeared in 219 samples (21.5%). Their largest
callers were lifecycle claims (72), notification batches (42), delivery status
reads (33), enqueue (17), inference polls (14) and responses (14). Most
multi-statement mutations require those transaction boundaries.

One bounded future option is to use the existing `PostgresDatabase.statement`
context for genuinely single-statement operations:

| Operation | Why one autocommit statement preserves its contract |
| --- | --- |
| `require_current_registration` | Its SELECT intentionally takes no row lock. Enqueue revalidates under its own transaction; this precheck only gates body admission. |
| Delivery status/body reads and periodic readiness reads | Each read is already one READ COMMITTED snapshot and carries no ownership authority. A separate BEGIN/COMMIT adds no snapshot consistency across these calls. |
| `_notify_loop` | One `SELECT pg_notify(...) FROM unnest(...)` publishes best-effort hints; authoritative changes committed earlier. Autocommit still delivers notifications only at statement commit. |
| `_claim_lifecycle` | The single CTE locks eligible lifecycle rows and updates claim tokens/leases atomically. Its locks must survive that statement and its implicit commit; no second statement currently depends on their continued ownership. A canceled/lost response remains a durable lease recovered after expiration. |

These candidates account for roughly 160 samples, or **15.7% of sampled GIL
work**. Applied to the measured 0.427 cores, completely removing that portion
would save only about **0.07 cores**; actual savings would be smaller because
autocommit still does work. This is an optimization opportunity to benchmark
later, not a requirement for the current qualification. Any change should
retain real-PostgreSQL cancellation, duplicate ownership, registration
replacement, lease recovery and delivery tests. No production code was
changed during this qualification for these candidates.

## Why the budget change can reduce CPU

The former 8 GiB budget admitted at most roughly 253 pending requests in the
observed run. Each additional guest retried HTTP admission about once per
second. The quota check runs after request/payload insertion and lifecycle
setup, so each rejected attempt performs SQL work before rolling back.
Removing that admission mismatch can reduce retry work even while accepting
more model requests. The measured before/after runs were not controlled
solely for this one setting, so the precise share of the CPU reduction due to
the budget change cannot be isolated from this evidence.
