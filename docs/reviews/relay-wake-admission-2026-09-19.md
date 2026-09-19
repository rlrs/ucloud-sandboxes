# Relay wake admission failure, September 19

Production retained five `super-park-harness` wake failures between 08:54:57 and
08:55:45 UTC with `HTTP 503: node startup concurrency is exhausted`. The relay
had already saved the model responses. Its lifecycle client retried 409 fences,
but immediately propagated retryable 503 admission responses to the worker.
That exposed temporary backend pressure as a failed response commit.

The worker has eight shared startup slots for creates, restores and file I/O.
Its twelve-slot restore ceiling is additionally constrained by that shared
budget; it does not permit twelve simultaneous restores. Sampled traces show
7–17 second worker restores during this run. Those observations establish
contention, not a complete attribution of the underlying I/O latency. The
previous 512 qualification used lightweight agents and an all-parked barrier;
it did not establish performance for this interleaved workload.

Server 0.5.45 retries explicitly retryable 429/503 wake admission responses in
the existing bounded relay wake dispatcher. Retries retain the operation ID,
back off with jitter and a bounded Retry-After, close each failed response,
and use at most ten minutes or the original request's remaining lifetime.
The model response remains saved and gated until wake succeeds; no model
request is resampled. Nonretryable errors and unclassified failures retain
the existing worker error path. Exhausted errors preserve the gateway error
code, and capacity retries are recorded in the wake trace.

The node admission budgets are unchanged. This patch runs on the gateway's
relay service; existing workers do not need a restart, and no SDK or Verifiers
update beyond the previously published versions is required for this fix.

Regression coverage includes 80 consecutive admission rejections followed by
success, request deadline exhaustion, socket closure, and unchanged handling
of terminal errors and optional parking. The canonical checks passed 910 server tests (six skipped), 114 SDK tests,
lint, Go/shell checks and installed-wheel verification. CI run 35434078547
passed after fixing a pre-existing test race: an HTTP response can arrive
before the handler's finally block releases its admission slot.

Runtime commit `05424e661328754f0b54afe55c14eeec393da74d` deployed at
09:15:27 UTC. All 90 installed package files matched the wheel and all services
were healthy. Two pre-existing parked sandboxes were preserved, with no pending
relay deliveries at deployment. The retry regression uses a simulated clock;
it exercises 80 failures without making the suite sleep for 80 seconds.

[Retained incident events and worker samples](../benchmarks/relay-wake-incident-2026-09-19.json).


## Production qualification

[Retained result](../benchmarks/relay-fast-512-0545-2026-09-19.json).

512 sandboxes passed five interleaved model/park/wake/tool cycles on workers
12396394, 12396395, 12396396 and 12396398, with 128 sandboxes per worker.
Cold creation took 180.3 seconds. Each Python agent retained and checked a
32-MiB allocation. The controlled upstream replied after 20 ms, so responses
arrived while trigger uploads and parking were still active. All 2,560 requests
received park and wake notifications and completed their tool checks.

The worker was restricted to **one response-commit attempt per request**:
all 2,560 succeeded. Upstream call counts were exactly 512 in each cycle,
with no resampling. Five retained admission-failure events (two startup-budget
failures and three snapshot-publication delays) correlate to subsequently
successful wake notifications and released responses. These are retained
state-transition events, not a count of every internal HTTP retry.

The complete trigger/delivery/tool cycles took approximately 54.1, 56.5, 49.5, 52.8
and 49.7 seconds. Tool verification began only after the relay had completed
all wake notifications, avoiding verification requests that could themselves
wake a parked sandbox. All 433 gateway and 433 relay health probes passed;
gateway p95/max were 271 ms/2.568 s, relay p95/max 19 ms/655 ms.
Periodic samples saw worker CPU reach 25%, with no swap use or storage errors.
Gateway-host CPU peaked at 82.7% including the load generator; available
host memory stayed at or above 2440 MiB. These are sampled observations.

This complements the earlier barrier-based 512-in-flight upstream test.
With fast replies, this run's upstream concurrency peaked at 30, while all
512 sandbox agents were live. It validates interleaved lifecycle handling and
backend backpressure, not 512 simultaneously CPU-saturated applications or
an external model service.

At 09:23:51 UTC, all test sandboxes and registrations had been cleaned up,
with no pending demand, reservations or relay deliveries. The two pre-existing
`super-park-harness` parked sandboxes remained unchanged. Both public services
reported healthy on 0.5.45. No additional SDK or Verifiers release is needed.
