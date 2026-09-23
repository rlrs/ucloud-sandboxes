# rc16 gateway list-probe failures

The pressure run `relay-load-cdf004f3586a` recorded HTTP 503 for public
`GET /v1/nodes` at 14:01:59.970, 14:02:05.280 and 14:03:01.128 UTC on
23 September 2026. The original probe used `raise_for_status()` without retaining
response headers or body. Its evidence cannot distinguish an ingress-generated
503 from the gateway's pre-handler request-capacity rejection.

The route reads the gateway's local heartbeat database. It does not fan out to
workers or query PostgreSQL. The four `resource_probe_failed` entries at
14:03:01 therefore mean one shared list request failed, not four independent
worker probes. Fleet qualification correctly retained that missing observation
as a failure; no retry was added to turn it into a passing observation.

Read-only gateway journals showed no SQLite error, handler traceback or automatic
restart in the affected interval. Systemd stopped the preceding gateway at
13:57:45 and started PID 548872 at 13:57:56. The `Serving gateway...` line appeared
at 14:01:46 from that same PID, without a systemd lifecycle event. It is consistent
with delayed stdout flushing, not evidence of another restart. NRestarts was zero.

A retained sampled Tempo span (`a2349c889c7a36650ef1767b91c36574`) records
`GET /v1/nodes` at 14:02:45.744 returning HTTP 200 in 17.972 ms, with 2.899 ms
thread CPU. This confirms a successful nearby request; sampling does not identify
the three failed requests. No gateway profiler or production mutation was used.
The historical failure cause remains unproven. There is no retained evidence
establishing a gateway database/storage failure.

Subsequent harness failures retain HTTP status, selected response headers
(including trace ID), at most 4096 body bytes with the supplied admin token
redacted, and any parsed error code/retryable flag. Collection reads at most 4097
bytes within 0.25 seconds. The original exception and fleet-health failure remain;
this is diagnostic capture, not a new retry or relaxed acceptance rule.
