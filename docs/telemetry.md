# Performance telemetry

The platform emits distributed traces and metrics over OTLP/HTTP. SQLite is
still used for controller facts that affect scheduling and for the lightweight
operator dashboard, but it is no longer a trace store. A trace backend such as
Grafana Tempo owns sampled spans; a metrics backend such as Prometheus, Mimir,
or VictoriaMetrics owns time series.

## Configuration

Telemetry is one exact block in deployment schema 5:

```json
{
  "telemetry": {
    "endpoint": "http://10.42.0.2:4318",
    "trace_sample_ratio": 0.1,
    "export_interval_ms": 5000,
    "export_timeout_ms": 3000,
    "max_queue_size": 4096,
    "max_export_batch_size": 512
  }
}
```

`endpoint` is an HTTP(S) origin, not an OTLP signal path. The processes append
`/v1/traces` and `/v1/metrics`. An empty endpoint disables telemetry and keeps
HTTP request handling on a single boolean branch. On Hetzner, use the gateway's
stable private address, listen only on the private network, and permit port
4318 only inside the deployment network. Workers receive the same settings in
their VM-init contract.

The recommended backend layout is:

- an OpenTelemetry Collector or Grafana Alloy receiver on the gateway;
- Tempo with its native S3 backend for trace blocks;
- Prometheus-compatible storage for metrics;
- Grafana for trace search, service graphs, and metrics-to-trace navigation.

Tempo should access Object Storage through its S3 client. Do not mount the
bucket as a filesystem. Telemetry retention is independent of sandbox snapshot
retention and should use a separate bucket or at least separate credentials and
prefix authority.

## What one trace contains

W3C `traceparent` and `tracestate` propagate across every synchronous boundary:

```mermaid
flowchart LR
  SDK["SDK request"] --> G["Gateway"]
  G --> W["Worker HTTP API"]
  W --> S["Privileged storage service"]
  S --> O["S3 or OCI registry"]
  G --> A["Autoscaler / provider"]
  SDK --> R["Model relay"]
  R --> P["Park caller"]
  R --> Q["Durable model queue"]
  Q --> K["Wake caller"]
```

The storage Unix-socket protocol is schema 4 and carries trace context as an
explicit field. Relay requests persist their original trace headers with the
durable queue item, so worker delivery and a later wake can be correlated even
after the caller connection is parked or recreated. Image-build and snapshot
publication threads capture context before leaving the request thread.

Important span groups include:

- `gateway.sandbox_create` and its image resolution, placement, pull, and node
  proxy phases;
- `sandbox.park`, checkpoint, artifact commit, runtime stop, storage release,
  and background publication;
- `sandbox.wake`, network setup, storage mount, runsc restore, readiness, and
  journal commit timings;
- `storage.client.*` and `storage.server.*`, including semaphore queue wait;
- `snapshot.export_and_upload`, `snapshot.compact_and_upload`, metadata commit,
  and verification;
- `image.build.worker` plus Docker build/push timings;
- `sandbox.exec.session`, covering the complete asynchronous process lifetime
  and exit status without recording commands, arguments, environment, or output;
- `relay.park_caller`, worker wait, and `relay.wake_caller`;
- `autoscaler.reconcile`, provider list/create/terminate, and `vm.bootstrap`.

Every sampled HTTP response includes `X-Trace-Id`; standard trace propagation
headers are also returned. IDs, lifecycle generations, byte counts, outcomes,
bounded phase timings, sampled thread CPU duration, and the corresponding
CPU/wall ratio are attributes or events. Credentials, request bodies, model
prompts, file contents, and S3 keys are not recorded.

Sampled spans lasting at least one millisecond carry
`ucloud.span.thread_cpu.duration` in seconds. Divide it by the span's standard
wall duration in the trace backend to obtain the CPU/wall ratio without
recording a redundant attribute. Shorter spans still pay the two clock reads,
but skip attribute storage because their individual CPU split is not useful.
This is a cheap screening signal: a long span with little thread CPU is waiting,
while a long span with a ratio near one is a candidate for code-level
optimization. Parent spans include CPU consumed by nested child work on the
same thread. For asyncio services, unrelated coroutines may also execute on
that event-loop thread while a span is suspended, so treat the ratio as
directional under high concurrency rather than as exact per-request accounting.

AgentEnv's ublk daemon exposes its own low-cardinality Prometheus metrics on
`127.0.0.1:9103`. A node-local collector can scrape that endpoint without
opening it to the deployment network. These native metrics cover the remote
block cache and ZFile behavior that application spans cannot explain.

## Performance invariants

Telemetry must lose data before it slows product work:

- span export uses one bounded in-memory queue and a daemon exporter thread;
- request threads only perform a non-blocking queue append;
- a full queue drops spans and increments `dropped_spans`;
- OTLP serialization, DNS, retries, and network I/O never run on request
  threads;
- trace sampling is parent-based and defaults to 10%;
- thread CPU uses two local clock reads only for sampled spans;
- metric labels are limited to operation name and outcome;
- no per-sandbox metric series are created;
- exporter failure cannot change an API, park, wake, build, or provider result.

`GET /v1/metrics` and the operator dashboard expose exporter queue capacity,
accepted/exported/dropped span counts, failed exports, and the last exporter
error. This health data is operational state, not a duplicate trace store.

The test suite freezes the exporter, saturates its queue, and verifies that a
burst of request-thread spans completes without waiting for the exporter.
The initial 100,000-operation microbenchmark is recorded in
[`benchmarks/telemetry-overhead-2026-08-13.json`](benchmarks/telemetry-overhead-2026-08-13.json);
at 10% sampling it measured 9.821 microseconds of instrumentation per synthetic
operation with no drops. A seven-run paired follow-up measured the sampled
thread-clock addition at 1.136 microseconds per operation at that 10% sampling
rate. This is an overhead guardrail, not a substitute for live park/wake
latency comparisons.

## First performance views

Start with distributions rather than averages:

- p50/p95/p99 `sandbox.park` and `sandbox.wake` by service version;
- park time split between runsc checkpoint, local sealing, publication queue,
  export/compaction, upload, and metadata commit;
- wake time split between remote verification/mount, runsc restore, and
  readiness;
- snapshot upload throughput as uploaded bytes divided by publication time;
- storage semaphore queue wait and concurrent publication queue wait;
- image build queue, Docker build, push, and context cleanup;
- provider create, VM bootstrap staging, and remote init separately;
- relay model wait versus response-to-wake latency.

Compare versions using `service.version` and deployments using
`deployment.id`. Resource attributes `cloud.provider` and `cloud.machine.type`
make Hetzner/UCloud and node-profile comparisons explicit. Alert on exporter
drops as well as platform latency: a fast graph with a saturated telemetry
queue is not trustworthy.
