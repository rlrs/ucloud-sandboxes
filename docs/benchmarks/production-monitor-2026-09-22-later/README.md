# Production follow-up: 2026-09-22, 08:29–08:31 UTC

Read-only observation of the active user workload, in UCloud project
4827bd3a-4e74-4393-9b82-49f71636c141. No deployments, restarts, configuration
changes or test workloads. UCloud authentication was refreshed locally and
management shells used for reads. The gateway has no current SSH update, so its
UCloud interactive shell was used; worker 12398499 also has an external SSH port.

Relay delivery gating remains slow. Two snapshots show 707 and 724 completed
wakes in the preceding five minutes, respectively:

| Metric | 08:29 UTC | 08:31 UTC |
| --- | ---: | ---: |
| Current routed sandboxes | 56 | 58 |
| Completion-to-delivery-release p50 | 5.90 s | 5.47 s |
| Completion-to-delivery-release p95 | 14.79 s | 12.19 s |
| Maximum | 26.23 s | 22.31 s |
| Pending wake deliveries | 11 | 13 |
| Oldest pending delivery | 6.77 s | 7.41 s |
| Local fleet API latency | 25 ms | 112 ms |
| Local relay stats latency | 49 ms | 40 ms |

The earlier 07:20–07:23 UTC observation had p95 16.06 s and 115–156 current
sandboxes. The current measurements do not establish a further latency regression.
Current fleet size is lower, but completed wake rate is similar, and these are
uncontrolled user workloads. Delivery release is not verified client tool latency.

An eight-second OS sample on each host finds worker storage contention:

| Worker | vda writes MiB/s | vda reads MiB/s | CPU iowait | I/O PSI some avg10 |
| --- | ---: | ---: | ---: | ---: |
| 12398499 | 108 | 248 | 17.8% | 32.3% |
| 12398500 | 450 | 142 | 4.6% | 14.5% |
| 12398536 | 1,517 | 133 | 11.5% | 20.1% |
| 12398539 | 1,665 | 250 | 12.9% | 31.6% |
| 12398540 | 1,078 | 269 | 19.0% | 30.6% |
| 12398541 | 1,016 | 229 | 22.3% | 43.7% |

Workers retain 32–55% CPU idle during these samples. There are dirty-page,
filesystem-lock and block-I/O waits, plus heavy page-cache reclaim. Physical
vda and its vda1 partition are the same traffic and must not be added together;
ublk traffic is another layer, not extra independent physical throughput.

All six workers report one active local compaction each. Their combined waiting
count changes from 44 to 50 between storage samples. Completed compactions increase
by 19, with one additional failure; output-size counters increase by 101 GB.
These counters are completion accounting, not a physical-disk attribution profile.
Remote publication is inactive on all workers. Worker 12398499's recent log contains
a native export control-response timeout and a separate compaction failure when a
sandbox runtime directory disappeared. These indicate wasted maintenance work;
they do not establish that compaction accounts for all physical I/O.

Gateway CPU idle is 45%, with 2.9 MiB/s vda writes. PostgreSQL has no current pool
queue, lock waiter, deadlock or temp spill at either snapshot. Across snapshots,
32 of 25,565 additional pool requests queued, accumulating 297 ms wait, and the
cumulative pool error count stays at six. PostgreSQL is not the primary bottleneck
in these observations. The builder is idle; no pending scheduling demand was
present in the routing snapshot.

The qualification package on the gateway lacks routing batching, warm-park grace,
compaction pacing and storage-journal batching. Worker 12398499's traceback also
shows the old export path without progress-aware control timeout. The recently
implemented fixes have not been deployed by this task. Worker maintenance pacing,
progress-aware export timeout and journal batching target the observed contention;
gateway batching alone will not resolve worker I/O pressure.

Artifacts: [first snapshot](monitor.txt), [second snapshot](monitor-2.txt),
[OS sample](os.json), [first storage counters](storage.json),
[second storage counters](storage-2.json), [routing and package details](details.txt),
[worker log](worker-12398499.log).
