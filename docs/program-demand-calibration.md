# Program demand calibration

`ProgramScaleSignals.calibration` reports a shadow estimate of the resources
parked sandboxes may need by the time another worker becomes ready. It does not
change provisioning, wake admission, parking, or the existing optional program
autoscaling calculation.

Memory comes from the existing resident sampler's last cgroup `memory.current`
observation. Heartbeats copy this cached value and its age; they do not open or
sample cgroups. The observation is usable only for the same node, provider job,
node boot (when recorded), sandbox generation, and spec hash. Both heartbeat and
sample must be within the existing live-pressure observation window. Missing,
stale, malformed, or mismatched evidence falls back to the declared memory bound.
CPU remains explicitly labeled as a declared bound; no CPU-throughput estimate
is invented.

For a model wait, the scheduler examines completed waits for that incarnation
already present in the request snapshot. It conditions those durations on the
current wait age and reports the fraction that would finish within the existing
measured provider startup p95. Missing startup or duration evidence contributes
the full demand. Ready-to-wake requests contribute their full demand. The
`observed_memory_sandboxes`, `unknown_memory_sandboxes`, `known_wait_sandboxes`,
`unknown_wait_sandboxes`, and `wait_samples` counters show coverage. This short
history is deliberately not a new durable prediction service; it must be measured
against subsequent ready demand before enabling decisions based on it. Concurrent
calls are still reduced to one future phase per sandbox by the existing scheduler.

Optional consolidation uses the same fenced memory observation. An observation
above the declared shape tightens the destination memory check. A smaller value
does not weaken the existing full-shape placement guarantee or authorize migration.

## Rollout and validation

Install gateway readers before workers emitting the optional inventory
`memory_observation` field. Older strict inventory decoders reject unknown fields.
Old workers remain compatible with the new reader, and invalid advisory payloads
are ignored without discarding sandbox ownership. No database migration or new
control endpoint is required.

`tests/test_program_calibration.py` covers observed/unknown demand, measured
provider lead time, incarnation and timestamp fences, duplicated history, old
inventory payloads, malformed advisory payloads, and the guarantee that shadow
results do not change actions. `tests/test_direct_runtime_assembly.py` separately
exercises both split backing modes through the real storage Unix protocol and
the assembled node HTTP heartbeat with a populated workspace registry.
