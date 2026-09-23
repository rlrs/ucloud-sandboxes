# Avoid a redundant pre-dispatch write for durable warm wakes

A running sandbox already owns its placement. For PostgreSQL-backed relay
requests, the response and wake intent are durable before the gateway is
called. The gateway no longer queues a separate observational SQLite write
before contacting that running owner. It records the original local response
and dispatch observation timestamps together with the eventual outcome.
Errors still persist a waking/error projection; non-running owners keep their
existing demand and placement path. Legacy non-durable requests are unchanged.

The worker generation/wake fence and route confirmation still commit normally,
and success still waits for the final local program projection. A gateway
crash before that final projection may lose the local pre-dispatch observation;
it cannot lose the authoritative PostgreSQL result or wake intent. On retry,
the first successfully persisted observation remains stable.

158 Linux routing/gateway regression tests passed. The eight load-harness tests
also pass, including rejection of forced mode without a separate control token.
The prior 256-sandbox result and next full-load result are separate evidence;
no subsecond qualification is claimed by this change alone.

Unprofiled run `relay-load-8b5f70f42178`, completed 00:26 UTC: 2,048/2,048
correct cycles. Measured wake p95 1.067 s, usable-tool p95 2.015 s; median
usable 0.738 s. The wake tail improved modestly, but overall usable latency did
not improve versus .95. Worker histories differ (three retained .96 workers
versus five at the start of .95); this is not a controlled causal comparison.
The gateway sample showed 34–44% idle CPU, low I/O wait and no TCP accept backlog.
The target remains unmet.
