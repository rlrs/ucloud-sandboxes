# Relay descriptor exhaustion and failed caller delivery

The September 18 follow-up found that the 0.5.34 relay was healthy at `/healthz`
while model delivery was failing. At approximately 14:22 UTC, recent metrics
showed 46 HTTP errors in 15 minutes, 177 completed responses awaiting delivery,
and request traces ending in 504 after roughly an hour.

The running relay and gateway both inherited a soft open-file limit of 1024.
The relay permits 4096 in-flight model requests, plus worker polls and lifecycle
connections. Relay logs since the 10:50 UTC deployment contained 244,144
`OSError: [Errno 24] Too many open files` lines, including failed outgoing
park/wake connections. The error count includes repeated server accept errors;
it is not a count of distinct failed model requests.

At 14:34:50 UTC, the live descriptor limits were raised to 65536 for both
processes using `prlimit`, without restarting either service. Systemd drop-ins
persist the setting, and the packaged gateway and relay units now include it.

A second failure amplified retries: every wake failure became HTTP 503 to the
model worker, including a definitive gateway 404 after sandbox deletion. At
14:33 UTC, all retained pending-delivery records referred to absent sandbox
routes. That is evidence of orphaned delivery at that time, not proof that the
original failures were all caused by deletion.

The code change closes the HTTP error response on every lifecycle failure.
Gateway 404/410, or explicit non-retryable 409, ends wake attempts for that
incarnation. The relay acknowledges the already committed model result and
durably releases delivery gating. It retains the original result for normal
authenticated replay and does not record a successful wake. Duplicate worker
responses, including after relay restart, do not retry that terminal wake.
Transient timeouts and 5xx failures remain retryable and retain delivery gating.

Validation before deployment: 872 server tests ran successfully, six skipped;
targeted CLI and relay suites passed, along with Ruff and diff checks. New
regressions cover response-socket closure, permanent versus transient gateway
failures, concurrent duplicate completion, durable restart/replay, preservation
of the sampled result, and successful retry after a transient wake failure.

The earlier 0.5.34 provisioning and consolidation smoke tests did not exercise
this relay connection volume. A responsive health endpoint did not establish
recovery of the model delivery path.

## Deployment and production verification

Code commit `2d3aa569c83ff0ee8695a1ae28565c93365e2dd5` was pushed and applied
as a gateway-host hotfix over 0.5.34 at **14:40:52 UTC**. The installed CLI,
relay module, and packaged service units match that commit. Only the relay
process needed to reload code; worker lifecycle code was unchanged. Both live
file limits and their persistent systemd overrides were verified at 65536.

The relay did not finish shutting down within systemd's 90-second stop timeout
and was killed and restarted by systemd. The durable database was backed up
before the restart and 53 outstanding requests were restored. No previous
database snapshot was written over current state. The relay was unavailable
during this restart; the gateway continued running.

Production checks passed:

- 1200 simultaneous keep-alive HTTP connections, with 1213 relay descriptors
  open, all returned healthy responses. Public health stayed responsive.
  Connections were closed after the test, which took 6.346 seconds.
- A newly created managed sandbox made an actual HTTP request through the
  public relay, was confirmed parked, then woke when the synthetic model worker
  supplied a result. Wake and completion returned HTTP 200 in 0.362 seconds.
  An assertion inside the restored sandbox verified the exact response body.
- An identical worker completion retry returned HTTP 200 in 0.001 seconds.
- The test sandbox was deleted. The initial rollout cleanup omitted its
  required registration fence and returned 400; a subsequent fenced cleanup
  succeeded. This was a smoke-harness error, not a delivery failure.
- Replayed 25 already committed results whose callers were absent, through the
  repaired `/worker/respond` API. All returned 200, stopped pending delivery,
  retained their original response exactly, and did not claim a successful wake.
  Completed responses awaiting delivery were then **zero**.
- No `Too many open files` errors were recorded after the 14:34:50 mitigation.
- [CI run 35357487452](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35357487452)
  passed on Python 3.10 and 3.13.

The 53 older requests still awaiting model work all referred to absent sandbox
routes. They were preserved under their existing expiry; this repair cannot
restore deleted callers or recover results already expired before intervention.
The live probes establish the repaired paths and descriptor headroom, not a
replay of the original complete workload.

Deployment hashes, the connection probe, database backup, and recovery evidence
are retained privately under `/work/ucloud-sandboxes/release/relay-20260918`.
