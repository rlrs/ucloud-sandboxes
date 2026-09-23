# rc11 forced parking latency

The production qualification `relay-load-b29507c8c32a`, 2026-09-23
12:26–12:28 UTC, completed 64/64 cycles with 48 measured parks. Measured
response-ready-to-continuation p95 was 1.518s and useful-tool p95 was 2.052s.
These are successful correctness results, not a passing subsecond cold-wake gate.

Nine retained sampled wake traces provide stage evidence; they do not include
the slowest measured cycles and cannot explain the overall p95 by themselves.
The full report is `architecture-candidate11-forced16.json`; selected worker
phase events are retained alongside this note.

- Across these samples, `runsc_restore` took 362–832ms (median 521ms).
  This includes sandbox startup and RAM population inside the restored
  sandbox's cgroup. Existing tracing does not separate those costs.
- Artifact validation took 29–87ms; cleanup took 16–35ms.
- Restore admission repeatedly took 52–55ms. The production metrics provider
  synchronously sleeps 50ms to sample CPU after its 200ms singleflight cache
  expires. This is a directly identifiable avoidable foreground delay.
- For the five sampled measured cycles, response-ready-to-dispatch was
  45–53ms, gateway wake HTTP duration 540–774ms, and dispatch completion to
  client continuation 97–119ms. Worker wake dominated these samples.
- One unmeasured initial-cycle sample spent about 300ms between gateway HTTP
  entry and worker wake, not explained by its 52ms admission timing. No
  stronger attribution is supported by the retained spans.

The first bounded fix uses the existing background resource-evidence collector
for CPU deltas. Request-path metrics still read physical memory and PSI, using
only their existing 200ms singleflight cache. CPU observations expire after two
seconds; startup, missing/reset counters and long collection gaps remain
unknown. No new pressure controller, pressure threshold, or heartbeat schema
is introduced. The obsolete foreground sample allowance in the admission retry
budget is removed. This saves an identified 50ms delay; it does not claim to
solve the remaining cold restore cost.

The next native measurement should separate RAM sparse-copy time/bytes from
remaining restore and inspect candidate cgroup CPU counters. Moving the copy
to a host helper would charge memory to the wrong cgroup and is not a valid
optimization.
