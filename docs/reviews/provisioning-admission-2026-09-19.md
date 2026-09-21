# Provisioning admission failures, September 19

The reported run created only 22 of 64 sandboxes. Production traces showed
`gateway.sandbox_ensure_image` receiving a 503 from a draining worker before
image preparation began. The node converted `SandboxAdmissionClosedError`
into `image_pull_failed`; the gateway then wrapped it in a nonretryable 502.
A routine admission race therefore terminated creation instead of moving it
to an available worker or waiting for capacity. Autoscaler stop activity in
the same window explains how a last ready heartbeat could still select a
worker whose admission had closed.

The failed image refresh had a separate admission problem. With no ready
builder, the gateway recorded build demand and returned an unclassified 503.
The SDK correctly avoided retrying arbitrary build POST failures, but had no
safe signal that this build had not started. The builder became ready about
45 seconds after submission.

Server 0.5.46 preserves `node_admission_closed` and retry headers at image
admission. Before dispatching a sandbox create, the gateway can remove its
provisional route and reselect a different worker. If none is ready, the
request remains retryable and its demand remains visible to the autoscaler.
Existing ambiguous create retries retain their original identity. The ready
node filter also excludes heartbeats explicitly reporting closed admission.

Cold and saturated builders return structured `builder_not_ready` or
`builder_busy` admission responses. SDK 0.4.23 retries only explicit,
retryable, pre-dispatch build admission failures, reusing the uploaded context.
The default context-upload and build-submission budget is ten minutes; an
explicit timeout overrides it. Unknown failures are not retried, avoiding
ambiguous duplicate builds. This budget does not cap the subsequent build
execution polling unless the caller supplies an overall timeout.

The server release is commit `3881876a76b92c8468d534e695c921b89c600777`;
SDK release is `335e368fc4fb30880445fed01e71f18e61cd2911` (v0.4.23).
Verifiers commit `61a313a` pins the published 0.4.23 wheel. Existing runner
processes must restart after updating their environment to load the SDK.

The canonical checks passed 913 server tests (six skipped), 118 SDK tests,
lint, Go/shell checks and installed-wheel verification. Verifiers passed its
11 tests and lint. Server CI 35435825281 and SDK CI 35435832323 passed.
The gateway, relay and autoscaler were restarted at 09:52:51 UTC; all 90
installed server files matched the release wheel. Future sandbox and builder
jobs use the repacked 0.5.46 bundles. Two pre-existing parked sandboxes on an
older worker were preserved.

The earlier 512 relay qualification did not cover this failing path: it reused
an existing image and did not explicitly exercise cold builder admission or
image preparation against a draining worker. It is not evidence that those
paths worked before this fix.

## Production qualification

[Full result](../benchmarks/provision-64-0546-2026-09-19.json) and
[drain/relay evidence](../benchmarks/provision-64-0546-evidence-2026-09-19.json).

The public-endpoint run used the published SDK 0.4.23, one asynchronous
sandbox client per sandbox, 64 concurrent creates, and two interleaved
relay/park/wake/tool cycles. Each agent retained a 32-MiB allocation and a
stable process identity. The controlled upstream responded after 20 ms.
This exercises the SDK transport used by Verifiers, not a full Verifiers
evaluation or an external model provider.

Starting with zero eligible ready builders, image creation completed in
50.268 seconds after 18 safely retried `builder_not_ready` responses. A second
build refreshed the same managed image in 5.147 seconds. All 64 created
sandboxes verified the new image marker before starting their agents.

The ordinary 64-sandbox reservation fit on one worker. To make the drain
race deterministic, the test reservation's disk headroom was temporarily
expanded to obtain a second empty worker. This accounts for the recorded
188-second capacity wait, including time spent adjusting the harness; it
is not a clean measurement of normal reservation startup latency. Only
64 sandboxes were created, with their original 5184-MiB writable disk size.

Admission was closed on empty worker 12396406 while its ready heartbeat was
still visible. All 64 creates succeeded on worker 12396409 in 19.344 seconds
(p95 17.989 seconds). Thirteen retained traces explicitly record
`reselect_after_image_admission_rejection`. Ten further closed-admission
responses reached the SDK and retried successfully. Other bounded startup
admission retries also occurred; none became terminal client errors.

Both cycles passed for all 64 agents: 128 durable responses, 128 accepted/park
notifications, 128 successful wake notifications, 128 tool executions, and
zero pending deliveries. Each response was committed exactly once and each
cycle made exactly 64 upstream calls. No model calls were resampled.

All 310 gateway and 310 relay health probes passed. Gateway latency p95/max
was 34 ms/1.948 s; relay p95/max was 10 ms/29 ms. The test removed its own
sandboxes, registrations and reservation and cleared its drain token.
A separate `super-park-harness` workload started after this run and was left
untouched. This qualification covers the reported 64-way provisioning
failure; the earlier 512 relay results remain a separate, narrower test.

## Subsequent workload observation

A separate 64-sandbox `super-park-harness` run started after qualification.
All 64 reached parked state. At 10:01:44 UTC, 30 wake demands were waiting;
at 10:02:37, 16 remained, labelled `wake_snapshot_publication_pending`.
The pending queue was empty by 10:03:33. Worker 12396406's last heartbeat
was temporarily stuck at 10:00:49 before recovering. Its node HTTP health
still responded during inspection; it had ample free disk/RAM, no swap use
and no reported storage-error volumes. The node journal contained delayed
publication/heartbeat responses whose clients had disconnected.

This is a separate performance concern, not proof of a terminal provisioning
failure or an established root cause. At 10:04:30, both workers and the builder
reported 0.5.46 with fresh, open-admission heartbeats, both public services
were healthy, and pending demand was zero. The independent run's full result
was not available, so this report does not claim it passed end to end or that
snapshot publication latency is fixed.
