# Park dispatch experiment, 2026-09-22

Production stayed on the two-vCPU gateway and release 0.5.79. The gateway,
PostgreSQL, workers, and workload were unchanged between runs. Only the relay
was restarted between A and B. B used the qualification launcher
`scripts/relay_dispatch_ab.py --park-concurrency 16`; A used ordinary dispatch.
The override was removed after B, and standard dispatch was restored.

Both runs used 256 agents, eight realistic dirty-memory/filesystem/model-wait
cycles, and natural adaptive parking. The first cycle is excluded from latency
statistics. All 2,048 cycles completed correctly in each run, and both cleaned
up their sandboxes.

| Metric | A: standard | B: 16 concurrent parks |
|---|---:|---:|
| Wake p50 | 0.884 s | 0.609 s |
| Wake p95 | 1.762 s | 2.204 s |
| Wake p99 | 2.307 s | 3.525 s |
| Response-ready to usable exec p95 | 3.104 s | 3.374 s |

This pair does not support reinstating a fixed park limit. It is not proof that
all dispatch admission strategies are ineffective: order, retention history,
and between-run variability remain confounders. It also does not establish the
cause of the regression from release 0.5.75's 1.504 s wake p95.

## CPU evidence

Five-second `/proc` samples include PostgreSQL processes as well as the gateway
and relay. For samples with more than 1.5 busy host cores:

| Mean utilization | A (11 samples) | B (10 samples) |
|---|---:|---:|
| Host busy cores | 1.93 / 2 | 1.87 / 2 |
| Gateway, percent of one core | 74.7% | 72.3% |
| Relay, percent of one core | 62.0% | 60.0% |
| PostgreSQL, percent of one core | 32.0% | 30.9% |
| Autoscaler, percent of one core | 4.0% | 3.5% |

CPU pressure rose to roughly 70–80%; I/O pressure was much lower in these runs.
The machine is actually CPU constrained, although PostgreSQL is not its largest
consumer. Four vCPUs could relieve contention; these measurements do not predict
its latency benefit or establish that resizing alone meets the 0.8 s target.
Software transaction/round-trip reduction is the next experiment.
