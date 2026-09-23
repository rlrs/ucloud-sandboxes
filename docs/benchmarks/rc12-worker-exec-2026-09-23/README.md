# rc12 retained slow exec-start traces

Read-only examination of natural512 run `relay-load-f612461fc77c`,2026-09-23
12:53–12:58UTC. No profiler or process attachment was used. `traces.json` contains
sanitized retained Tempo spans; `matching-cycles.json` contains exact matching
client samples for two slow operations.

| Same exec-start request | Sandbox0014 cycle1 | Sandbox0087 cycle1 |
| --- | ---: | ---: |
| SDK measured exec-start | 1,724ms | 1,724ms |
| Gateway HTTP span | 1,608ms | 1,601ms |
| Gateway waiting for worker headers | 1,540ms | 1,447ms |
| Worker HTTP span | 1,308ms | 1,357ms |
| Worker `node.sandbox_exec_start` | 1,248ms | 1,246ms |
| Current thread CPU inside worker start | 49ms | 43ms |
| Nested ensure-running total | 117ms | 226ms |
| Nested exec-lease acquisition | 13ms | 49ms |

Most elapsed time is **inside the worker's `ExecSessionManager.start`**, after
request handoff. Only115–123ms lies outside the gateway span; ingress/SDK delay
cannot explain the1.7s samples. Existing ensure-running and exec-lease timings do
not explain about1.0s inside worker startup. That remainder includes lifecycle
entry, registration/record reads, capacity sampling, global session registration,
subprocess launch and pump-thread startup. The low current-thread CPU establishes
waiting/scheduling as a large part of elapsed time, but does **not** distinguish
lock contention, GIL scheduling, subprocess/thread creation or I/O. There is no
profile evidence assigning the remainder to one of those causes.

Worker startup ends before command completion. Matching guest tool times were
64ms and62ms; optimizing that tiny guest command cannot remove this start delay.
The bounded next measurement is stage timings around lifecycle entry, record
lookup, capacity acquisition, registry insertion, Popen and pump-thread startup.
A gateway async upload handoff may reduce shared load, but these spans do not
establish it as the main cause of exec-start delay.
