# Architecture load qualification

These summaries retain successful and failed candidate runs. `correct` covers
operation completion, integrity, API-health checks and cleanup; consult
`fleet_health` separately for placement-scoped resource/heartbeat checks.
`slo_passed` also includes latency and fleet-health gates. A failed latency gate
must not be described as passing the original performance target.

The canonical harness is `scripts/live_relay_load_benchmark.py`, run on Linux
with released SDK 0.4.26 against an idle production fleet. Authentication is read
from private token files, never included in these reports. Reports preserve
configuration, candidate versions and the SHA256 of the full report.

Common arguments: rolling startup, 16 concurrent creates, 24 fleet pollers,
20–25-second model waits, 1,800-second workload deadline, 64 files of 64 KiB,
64 KiB tool upload, 32 KiB response, 100 ms CPU work, 4 GiB workspace. Heap
integrity is checked after every continuation and usable tool. Earlier candidate runs kept the agent alive holding its heap after its last
cycle until final cleanup. That cannot reach a completed resident state when all
finished heaps exceed physical RAM. The corrected harness exits after its final
result and requires an authoritative successful terminal record for every primary.
All cycles, allocations and integrity checks remain; global sandbox cleanup still
runs afterward. Reports record the harness SHA256 and completed scenarios.

| Profile | Sandboxes × cycles | Heap / dirty MiB | Bound MiB | Parking |
| --- | --- | --- | --- | --- |
| natural512 | 512 × 8 | 512 / 128 | 1024 | Natural |
| pressure256 | 256 × 8 | 1536 / 384 | 2048 | Natural |
| pressure_correctness256 | Same as pressure256 | Same | Same | Natural |
| pressure_integrity256 | Same as pressure256 | Same | Same | Natural |
| sustained512 | 512 × 32 | 512 / 128 | 1024 | Natural |
| repository256 | 256 × 8 | 512 / 128 | 1024 | Natural |
| repository_forced16 | 16 × 4 | 512 / 128 | 1024 | Forced |

`pressure_correctness256` changes only
`--sandbox-request-timeout-seconds 1800` from the default 180. It tests eventual
completion under pressure with the original overall workload deadline; it is
not equivalent to passing the original request-latency requirement. Independent
cycle/probe limits and all integrity checks remain. Cleanup follows the workload
under its own bounded operations.

`pressure_integrity256` additionally sets `--continuation-timeout-seconds 1800`.
It isolates eventual crash/integrity completion from the failed original
three-minute continuation deadline. The overall workload deadline remains 1,800
seconds, all cycles and terminal-state checks remain mandatory, and the original
latency SLO thresholds are unchanged. These results cannot be reported as a pass
of `pressure256` or used to erase its timeouts.

Repository profiles additionally use 64 FULL-synchronous SQLite transactions of
16 KiB per cycle, retained reader/writer state and 256 files. Forced parking
proves restoration integrity independently of a natural policy that may retain
all guests. The 1.5 GiB pressure profile exceeds aggregate physical worker RAM
before runtime overhead, so it necessarily exercises admission and reclamation.

See the [completion audit](../../reviews/performance-architecture-completion-audit-2026-09-23.md)
for acceptance status and the [release closeout](../../reviews/release-0.5.114-2026-09-23.md)
for deployed scope. Failed reports remain evidence, even when a later candidate
fixes their cause.
