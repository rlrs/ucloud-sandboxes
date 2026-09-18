# Production health investigation — September 18, 2026

Scope: DFM Pretraining, deployment `live-ucloud-20260824a`, server 0.5.33.

## Findings and changes

1. **Builder capacity was capped at one node.** At 07:59 UTC, the workload had
   requested 16 builders and had five pending builds. The single builder had
   four active builds, its per-node concurrency limit. Production
   `builder.max_nodes` was raised from 1 to 4 after backing up the configuration.
   The autoscaler successfully bootstrapped three additional builders.
2. **Builder routing ignored active build count.** New nodes had zero active
   builds while the original remained full. Builders reported equal nominal
   free resources, so the gateway's node-ID tiebreaker kept choosing the same
   node. New builds now prefer fewer active builds, then more physical free
   disk. A live smoke check exposed the 20-second periodic-heartbeat delay;
   selection now refreshes load directly from authenticated nodes and excludes
   draining, restarted, or unresponsive candidates. Before balancing, the gateway checks existing build records by image
   ID and routes retries to the active owner, retaining node-local deduplication
   and conflicting-spec checks. An indeterminate owner lookup defers dispatch.
3. **Operational messages understated failures.** The autoscaler now states
   when requested builder capacity exceeds the configured cap. The health
   report gives the error share and no longer infers recovery merely because
   some requests succeeded. Initial gateway build metrics recorded 1,822 error
   operations and 69 successful operations over 30 minutes; these count API
   attempts, including retries, rather than distinct failed images.
4. **Some workload images are invalid.** Retained failed builds included 11
   Dockerfiles copying `task_file` absent from their uploaded contexts, another
   missing `/repo/requirements/framework.txt`, and package/bootstrap command
   failures. These require fixes to the submitted workloads. They were not
   silently retried or modified during platform repair.
5. **Tempo readiness was intermittent.** The first check returned HTTP 503;
   repeated direct checks and the 08:08 UTC report were healthy. Tempo had no
   systemd restarts. This investigation did not establish the transient cause.

## Deployment and validation

The gateway, autoscaler diagnostics, and report script were applied as a
three-file hotfix over 0.5.33. Before installation, each installed source hash
matched the repository baseline. Installation retained original files and a
SHA-256 manifest, checked Python syntax, restarted the gateway/autoscaler, and
verified health. Automatic source rollback was prepared for health failure and
was not needed. Gateway restart was authorized by the user.

The public gateway and relay health endpoints report 0.5.33. At 08:08 UTC the
Collector, Tempo, VictoriaMetrics, and Grafana were all healthy; the pending
build queue was empty. The workload had become idle, so that zero error rate
cannot establish a throughput improvement. The builder cap is a maximum;
normal idle scaling remains enabled.

Local validation: 155 tests passed across builder selection, policy,
reconciliation, observability, gateway, image builds, and correctness
regressions. Ruff checks and `git diff --check` passed.

Production backups and detailed evidence are retained on the gateway under
`/work/ucloud-sandboxes/release/health-20260918/`. Repository changes should be
included in the next versioned release; this hotfix did not publish a new
package or rebuild worker bundles.

Final verification at **08:16 UTC (10:16 CEST)** found all four core services
active, all four telemetry backends healthy, no pending sandbox or build demand,
and no immediate faults in the two-minute report. The isolated live test built
two images successfully on different builders and retried the first request
without creating another build. The first test exposed stale periodic load;
the passing test exercised the subsequent direct-load refresh fix. Test image
tags and image-store entries were removed; temporary builder preparation was
removed. Build history remains as audit evidence. There was no representative
production load during this final check.

## Provisioning timeouts follow-up

The user supplied both nginx HTML 504 and JSON `stream timeout` errors from
sandbox provisioning. A targeted Tempo search over 06:00–08:20 UTC returned
60 sampled traces longer than 30 seconds: 52 sandbox creates and eight exec
polls. No other gateway endpoint appeared in that result. This is sampled
telemetry, not proof that every other request was fast.

The longest creates took 1,800.2 and 1,785.3 seconds. They spent essentially all
of that time in image preparation, with only 22–33 milliseconds of request
thread CPU time, then failed to write their response because the client had
already disconnected. Other creates spent 74–96 seconds pulling images and
about one second actually creating the sandbox. These traces establish long
blocking waits; they do not establish gateway CPU saturation.

Two mechanisms amplified the waits:

- The per-node/image pull lock had no wait deadline. Creates retained their
  HTTP thread and one of 32 create-admission slots while another pull held it.
- Node connections and connection-pool acquisition inherited the operation's
  timeout, including the 30-minute image-pull timeout.

The gateway hotfix now shares at most 32 background create-image tasks and
waits at most two seconds for image preparation per HTTP attempt. It returns
structured HTTP 503 `image_warmup_pending` with `retryable: true` and
`Retry-After: 2` while work continues. The existing SDK recognizes this
response. The durable sandbox route, generation, and operation ID remain
assigned; retries check image readiness before dispatching create. An
independent registry lease protects the image if the request is canceled.
Connection and connection-pool waits are capped at five seconds, while valid
long operation read timeouts remain available.

Validation: 164 tests passed, including concurrent pull deduplication,
bounded background capacity, failure cleanup, timeout separation, and an HTTP
integration test proving repeated pending responses release admission,
preserve identity, keep health available, and eventually dispatch one create.
Ruff and `git diff --check` passed. The deployed source SHA-256 is
`db1b07300094652ba61fecd238dd07f99029e8903c9fa928c8b39000e0d5674a`.
Pre-install source verification, backup, syntax checks, and health-failure
rollback were retained in `health-20260918/hotfix3` on the gateway. The approved
brief restart completed and health recovered.

A separate worker-loss event remains unexplained. Job `12395318` was reported
powered off by UCloud at 07:36:30 UTC while the autoscaler still observed active
sandboxes. The autoscaler then submitted a stop for the suspended/lost worker
at 07:36:32. There was no configured job expiration. This explains some
unreachable-worker requests but does not identify what powered off the VM.
The hotfix does not claim to repair that underlying provider/worker failure,
or to cap the entire create lifecycle: actual worker creation retains its
existing timeout and identity fencing.

Live verification through `https://app-sandboxes.cloud.sdu.dk` created a test
sandbox from the cold `docker.io/library/python:3.12-slim` image after scaling
from zero workers. Across 20 attempts, no-ready-node responses were prompt;
the cold-image response returned `image_warmup_pending` in 2.025 seconds, and
the successful retry completed in 0.764 seconds. All 30 concurrent public
health probes succeeded, with maximum latency 0.027 seconds. The test sandbox
was deleted successfully. Evidence is retained in
`health-20260918/provision-timeout-smoke.json` on the gateway. This is a
controlled cold-start check, not a replay of the original production load.

Final check at 08:39 UTC: all four core services were active, all four telemetry
backends were healthy, and pending sandbox/build demand was zero. The two-minute
report still warned about the smoke test's expected no-ready-node and image
warmup responses; these are counted as operation errors by current telemetry.
The successful create, explicit retry responses, and independent health probes
are the verification evidence, not a claim of zero errors under real load.

## Capacity follow-up

The [capacity investigation](production-capacity-2026-09-18.md) records worker
utilization, the supplied Verifiers integration's parking behavior, and a
subsequent worker wake-race fix. That later fix updates the future worker
bundle; the earlier gateway-only hotfix description above is historical.
