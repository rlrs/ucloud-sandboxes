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
