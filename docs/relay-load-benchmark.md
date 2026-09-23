# Reproducing managed-agent load

`scripts/live_relay_load_benchmark.py` drives real SDK managed processes through
relay request acceptance, checkpoint parking, response delivery, restoration,
and tool execution. It uses synthetic model responses, so it needs no model API
or production user workload.

Run it against an **idle deployment**, from a separate driver machine. The script
refuses an occupied fleet. Use the deployment's sandbox API token and relay
worker token in private files; do not put token values in command arguments.
The image must contain Python and should be pinned by digest.

```sh
.venv/bin/python scripts/live_relay_load_benchmark.py \
  --gateway-url https://GATEWAY \
  --relay-url https://RELAY \
  --sandbox-token-file /private/path/sandbox-token \
  --relay-worker-token-file /private/path/relay-worker-token \
  --image python@sha256:IMAGE_DIGEST \
  --sandboxes 256 --cycles 8 \
  --output /private/path/relay-load.json
```

Start with four agents, then 64, then 256. The driver waits for every agent to
start before releasing the first round. Each agent keeps 128 MiB of random
resident memory, dirties 16 MiB of pages per round, rewrites 64 files of 64 KiB,
and performs 100 ms of CPU work. A synthetic model response takes 10–15 seconds.
These parameters are configurable; match the real workload's working set and
model delay before treating the result as representative. The test image runs
as root inside its sandbox, matching the existing managed-agent fixtures.

The default `--parking-mode natural` sends the response as soon as the synthetic
model is ready. `--parking-mode forced` instead requires every round to reach the
gateway's parked state before submission. After delivery, the agent checks its memory digest, file contents,
process identity, response identity, and a subprocess tool result. The driver
also runs a separate SDK exec and verifies the result. Requests use stable
idempotency keys across guest transport retries.

Forced parking may extend the configured model delay. The primary
`response_ready_to_usable_exec_seconds` timer includes that wait and driver
scheduling delay. Report version 2 separates durable acceptance from observed
guest continuation; old commit-as-wake aliases are removed. In natural mode a response may prevent an unnecessary park;
`park_observed_after_seconds` is null if no current park was observed.

The report separates:

- `response_ready_to_usable_exec_seconds`: scheduled synthetic model readiness
  through the successful SDK exec. This is the acceptance metric in both modes.
- `response_ready_to_submit_seconds`: driver delay and any forced-park wait
  before response submission.
- `response_commit_seconds`: before SDK submission through durable acceptance,
  including client admission and retries. Acceptance is not wake completion.
- `response_ready_to_commit_seconds`: model readiness through durable acceptance.
- `response_ready_to_guest_continuation_seconds`: model readiness through observing
  a guest-originated receipt after response validation and its first tool.
  This includes observation-tunnel transit/polling; it is an upper bound on guest
  continuation, not a pure runtime restore measurement.
- `usable_exec_seconds`: the same starting point through a successful SDK exec
  observing the agent's verified tool result.
- Parking observation delay, delivery count, worker placement, health latency,
  correctness errors, and cleanup errors.

Before issuing any external upload or exec, the driver requires that receipt via
a separate, unbound relay rollout. This instrumentation cannot request park/wake.
Reading sandbox files or managed logs would itself wake a parked sandbox and
invalidate the measurement. The receipt carries process nonce, cycle and response
digest. The observer timestamps a received batch before acknowledging its items;
its additional traffic/overhead is part of the reported conservative measurement.

The event log records every event. The JSON report is checkpointed once per
second and fully written at completion; it records the harness SHA256.

For release qualification, supply `--gateway-token-file /private/path/gateway-token`.
Report version 3 adds a separate `fleet_health` gate using the existing cached
`/v1/nodes` probe; it does not poll workers directly. Every worker currently
hosting this run's known placements must provide both a heartbeat and resource
sample no older than 30 seconds after its initial 30-second placement grace.
Repeated inventory polls cannot reset the grace period. Unused workers do not
affect this gate. Missing nodes, stale timestamps, missing resource metrics, and
failed admin probes produce typed `fleet_health_failures`; later recovery does
not erase a failure.

`correct` continues to describe workload correctness. With admin sampling
enabled, `slo_passed` additionally requires `fleet_health.passed=true`; fast
responses cannot hide broken worker heartbeats. A run ending entirely within
the grace period is insufficiently observed and cannot qualify. Without admin
credentials, fleet health is explicitly `unknown`, while ordinary client latency
checks remain available. Existing latency definitions and samples are unchanged.

The first cycle per agent is warmup. Remaining samples produce median, p95, p99,
and maximum latency. Exit status zero requires all cycles and cleanup to succeed,
all health checks to pass, **observed guest continuation p95 below 0.8 seconds**,
and **response-ready-to-usable-exec p95 below one second**. Configure these with
`--continuation-p95-seconds` and `--useful-action-p95-seconds`. A correct but
slower baseline exits nonzero; inspect `correct` and `slo_passed` separately.
This includes the first subprocess tool and its external SDK confirmation, plus driver network latency,
so it is stricter than a worker-only restore timer. Full resident-memory and file
integrity checks are mandatory separate correctness gates after that timer.

Cleanup targets only the run's unique `relay-load-...` IDs and reservation.
Cancellation also enters cleanup. If the driver is forcibly killed or its host
is lost, use the recorded run ID to remove only that run's resources and relay
registrations. Keep the driver outside the autoscaled worker/builder fleet.

For diagnosis, capture gateway placement-lock timing, relay lifecycle-lock
waiting, worker restore timing events, node-agent thread stacks, and host CPU,
I/O, and memory pressure while the run is active. Compare the same workload and
worker placement before and after a candidate; a light-load smoke test does not
qualify the loaded latency target.

Measurements and rejected experiments are recorded in
[the 21 September investigation](benchmarks/relay-load-2026-09-21/README.md).

For candidate experiments that must cover autoscaled workers, pass
`--start-signal-file /private/path/unique-start-file`. The file must not exist.
After `all_agents_ready` appears in the event log, verify the candidate on every
worker listed in `placements`, then create that file to release model traffic.
The overall deadline and exact-ID cleanup still apply while paused. This avoids
quietly benchmarking a mix of patched workers and newly booted release workers.

## Release acceptance profile

The production-like profile used in September has 512 MiB resident memory,
128 MiB dirtied each cycle, 64 files of 64 KiB, 32 KiB model payloads,
100 ms CPU work, and 20 seconds plus up to 5 seconds of model delay. Use
16 concurrent creates and 24 concurrent fleet pollers. Explicitly pass:

```sh
--resident-mb 512 --dirty-mb 128 --model-seconds 20 --model-jitter 5 \
--create-concurrency 16 --fleet-pollers 24 --useful-action-p95-seconds 1
```

Qualify actual forced parking with 64 agents and four cycles, then immediately
run 256 agents and eight cycles with `--startup-mode rolling --parking-mode natural`.
Repeat against the already used fleet, and extend to 512. A passing fresh-worker
run alone is insufficient: reused/dense workers have exposed memory-compaction
stalls that short component tests missed. Natural warm retention can yield zero
observed parks; it does not qualify restore. Every forced measured cycle must
actually park. Keep launch-overlap and steady-state latency visible separately.

Do not change worker tuning or attach a profiler during an acceptance run. Use
separate diagnostic runs for profiling. Retain correctness, cleanup, health,
placement, kernel compaction/pressure evidence, runtime identity, and raw reports
for both passing and failed runs. Record experiment tradeoffs and rejected
changes, including filesystem throughput regressions.

Before a gateway upgrade, also validate retained heartbeat records with the
candidate package, not merely `/healthz` after starting it:

```sh
PYTHONPATH=/path/to/candidate.whl python scripts/verify_heartbeat_upgrade.py /path/to/control.sqlite
```

This read-only check exercises the persisted canonical heartbeat schema. It
complements live HTTP and Linux tests, including old-format records and failed
writer/ownership paths. It does not replace them.

## Repository and SQLite WAL profile

Add the following to the release acceptance profile for a repository-heavy
agent. This is the same driver and lifecycle path; SQLite is disabled by default.

```sh
--files 256 --file-kib 64 --sqlite-transactions 64 --sqlite-payload-bytes 16384
```

Each cycle commits 64 separate transactions with `synchronous=FULL`, using
incompressible deterministic payloads. An open writer and an old reader snapshot
retain the WAL and shared-memory index across every model wait. The guest reads
both connections before its continuation receipt; losing rows or changing the
reader snapshot fails the run. After continuation, it runs `integrity_check`,
opens a fresh connection, and checks every committed payload. Results record
`sqlite_rows` and `sqlite_digest` per cycle. Full scans remain in the separate
mandatory integrity gate, rather than being mislabeled as restore time.

Qualify this profile with actual forced park/restore cycles before larger natural
and sustained runs. Increasing `--cycles` retains more WAL history deliberately;
size disk reservations accordingly. The local Linux fixture verifies live
connections over multiple cycles and recovery from copied DB+WAL files without
the shared-memory index. That fixture does not replace the native park/restore
qualification. A DB-only copy is checked to be missing the committed rows, so a
passing fixture cannot accidentally omit the WAL dependency.
