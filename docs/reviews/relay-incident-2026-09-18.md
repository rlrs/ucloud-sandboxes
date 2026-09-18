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
