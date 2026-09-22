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
scheduling delay. The older submit-to-wake timers remain for comparison with
previous reports. In natural mode a response may prevent an unnecessary park;
`park_observed_after_seconds` is null if no current park was observed.

The report separates:

- `response_ready_to_usable_exec_seconds`: scheduled synthetic model readiness
  through the successful SDK exec. This is the acceptance metric in both modes.
- `response_ready_to_submit_seconds`: driver delay and any forced-park wait
  before response submission.
- `commit_and_wake_seconds`: before SDK response submission through successful
  relay commit/wake acknowledgment, including client admission and retries.
- `usable_exec_seconds`: the same starting point through a successful SDK exec
  observing the agent's verified tool result.
- Parking observation delay, delivery count, worker placement, health latency,
  correctness errors, and cleanup errors.

The event log records every event. The JSON report is checkpointed once per
second and fully written at completion; it records the harness SHA256.

The first cycle per agent is warmup. Remaining samples produce median, p95, p99,
and maximum latency. Exit status zero requires all cycles and cleanup to succeed,
all health checks to pass, and **response-ready-to-usable-exec p95 below one second**. A correct but
slower baseline exits nonzero; inspect `correct` and `slo_passed` separately.
This deliberately includes post-restore verification and driver network latency,
so it is stricter than a worker-only restore timer.

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
