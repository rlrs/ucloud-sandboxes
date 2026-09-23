# Gateway interruption during rc8 natural-64

The owned `candidate8-natural64` trial completed all 256 application cycles, but
failed health and latency qualification. Measured useful-action median was
0.437 s and p95 10.943 s; response-commit p95 was 9.209 s. No parks were observed.
This failed trial remains in the comparison; the interruption is not removed
from its percentile calculation.

## Retained evidence

All times below are UTC on 2026-09-23. Investigation used retained journal and
VictoriaMetrics samples, with no live profiler or worker fanout.

| Time | Observation |
| --- | --- |
| 11:42:26.530–26.780 | Gateway SSH authentication, PAM, systemd user startup and session creation progressed normally. |
| 11:42:27.471 | `sudo journalctl -u ucloud-sandbox-node --no-pager -n 1` began in that SSH session. |
| 11:42:28.531 and 33.842 | The independent load driver received HTTP 503 from `/v1/nodes`. A health request also hit its five-second timeout. |
| 11:42:37.465 | Gateway kernel: `clocksource: Long readout interval, skipping watchdog check: cs_nsec: 10431374645 wd_nsec: 10431374640`. The reported interval is **10.431 seconds**. |
| 11:42:37.683 | The SSH command's sudo session closed, about 10.212 seconds after its command log. |
| 11:42:37 onward | Autoscaler, SSH and systemd messages resumed; lifecycle requests completed in a burst. |

The gateway's retained `job="victoria-metrics", instance="self"` process
pressure counters changed as follows. These are the exporter’s scoped pressure
counters, not per-sandbox measurements:

| Counter | 11:42:24.402 | 11:42:44.402 | Increase |
| --- | ---: | ---: | ---: |
| `process_pressure_io_waiting_seconds_total` | 17.449965 | 25.497865 | **8.047900 s** |
| `process_pressure_cpu_waiting_seconds_total` | 81.614150 | 81.647041 | **0.032891 s** |
| `process_pressure_memory_waiting_seconds_total` | 0 | 0 | **0 s** |

Bootstrap output for another worker appeared together at 11:42:37, but this does
not timestamp when those bootstrap operations ran. `run_init_over_ssh()` captures
the entire SSH command's combined output and prints it after the command returns.
The captured worker phases span 34.144 seconds. Their identical gateway journal
timestamps therefore cannot establish a local bootstrap CPU burst as the cause.

PostgreSQL started its scheduled checkpoint at 11:42:15.189 and completed at
11:43:51.825: 873 buffers, 96.627 seconds of paced writes, 0.005 seconds of sync.
This overlaps the incident but does not establish causation. Retained sysstat
sampling is too coarse to distinguish a ten-second event; its 11:40 sample
precedes the interruption.

## What this establishes

The interruption extended beyond Python request handling: an independent SSH
command stalled and the kernel reported the matching long watchdog interval.
The retained exporter also recorded substantial I/O waiting during this window,
with little reported CPU waiting and no memory waiting. An HTTP ingress-only or
single application-lock explanation does not account for all of this evidence.

The evidence does **not** identify the initiating event. It cannot distinguish a
guest storage/kernel stall, underlying storage interruption, or VM scheduling/
pause event. There is no justified attribution to UCloud, Python, PostgreSQL or
bootstrap work. The bounded next diagnostic is host-side VM/storage scheduling
evidence for 11:42:27–38, correlated with these timestamps, plus an otherwise
unchanged repeat after worker bootstrap finishes. The existing load gate must
continue to fail when fleet health disappears, even if application cycles recover.

The subsequent rc8 natural-64 repeat completed 256 turns with healthy fleet
telemetry, continuation p95 0.310 s and useful-action p95 0.786 s. It observed
zero parks. This demonstrates recovery and a passing warm trial; it neither
identifies the interruption's cause nor qualifies forced restore or greater load.
