# rc19 forced-wake attribution

The 16-sandbox production qualification completed 64 cycles with workload and
fleet-health checks passing. Continuation p95 was 1.221 s and useful-exec p95 was
1.719 s; this still misses the latency target.

The retained Tempo sample contains six complete wake traces from 16:56:53–
16:57:49 UTC, matched to their exact driver cycles. It is a sampled diagnostic
set, not a fleet percentile estimate. Raw spans and exact matches are retained
in `rc19-forced16-wake-traces.json` and `rc19-forced16-wake-attribution.json`.

| Boundary | Six-trace range |
|---|---:|
| Gateway admission | 4–16 ms |
| Worker restore queue | 2–13 ms |
| Mount/validate/source quota preparation | 59–115 ms |
| Native restore | 144–341 ms |
| Artifact cleanup after durable RUNNING | 58–309 ms |
| Warden total | 330–840 ms |
| Worker Python thread CPU | 25–64 ms |
| Continuation observation after gateway wake returns | 186–358 ms |

The slow matched example is sandbox `relay-load-16cc1e9ecac8-0000`, cycle 2,
trace `1a6075c991bdb57195b5fecf118cf09`. Response-ready was
16:57:23.064420 UTC. Worker wake began 89 ms later. Native restore took 322 ms,
artifact cleanup 309 ms and mount/validation 115 ms. Warden finished in 840 ms;
the complete gateway wake request took 909 ms. The continuation observer saw the
guest 247 ms after that response, producing 1.195 s total continuation latency.
Gateway admission was 4 ms, so adding gateway capacity or changing admission
limits is not supported by this example.

The cleanup region is concrete avoidable foreground work. After the paused
candidate is verified, resumed, passes readiness and has durable RUNNING,
`_finalize_restore_artifacts` synchronously deletes the old complete generation,
releases the source's retention project and releases its physical overlap claim.
On the loop-backed XFS deployment, project release runs the existing coalesced
FITRIM barrier before claim release, then clears quota and journals the result.
The current timing groups those operations, so attributing all 309 ms specifically
to FITRIM would be unsupported. Source inspection identifies it as part of the
measured region, not a separately measured cause.

The smallest justified follow-up is to retire that complete generation after
RUNNING through the existing maintenance/reconciliation owner, keeping the
retention project and physical overlap claim until cleanup succeeds. It must
retain exact incarnation/generation/digest fences and handle crash, next capture,
delete and concurrent publication readers. It must not release the claim early
or turn failed trim into success. Add cleanup subphase timing to distinguish
unlink, quota/trim and journal cost before pursuing narrower filesystem changes.
This could remove the measured 58–309 ms from foreground wake; it does not imply
that native restore, lazy guest page faults or observation transit become free.

The final 186–358 ms includes release of the relay response, guest Python first
tool execution and usable-result write, a second observer relay submission, and
the driver's observer poll. The harness documents that measurement as an upper
bound on guest continuation. No trace here isolates those components. Likewise,
144–341 ms native restore cannot be split into clone, runtime startup and kernel
state restore from the current spans alone. Isolated native results establish
that the complete application heap is not eagerly copied, but do not establish
that every production restore should take 114 ms.

Nearby heartbeats reported substantial available memory and modest CPU use,
with I/O PSI around 1–8%; their sample times precede individual restores, so they
are context, not synchronized proof of a particular blocked syscall. This
analysis used retained traces and reports only. No worker profiler, configuration
change or extra workload was run during the 256-sandbox pressure trial.
