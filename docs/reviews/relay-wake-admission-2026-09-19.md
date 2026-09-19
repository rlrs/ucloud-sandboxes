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
of terminal errors and optional parking. Live deployment and qualification
results will be recorded below.
