# Production monitoring — 2026-09-22, 07:20–07:23 UTC

Read-only samples of the user workload after the PostgreSQL relay cutover.
No synthetic load or configuration changes were applied.

Fleet grew from 115 to 156 sandboxes between samples; 45 were still creating
at the second sample. Among 740 completed wakes in the preceding five minutes,
PostgreSQL completion-to-delivery-release timestamps measured p50 5.82 s,
p95 16.06 s and maximum 27.25 s. This measures relay delivery gating, not verified
client tool execution. Pending wakes grew from 21 to 36; oldest grew from
13.73 to 21.56 seconds. These are poor loaded latencies despite successful delivery.

Worker 12398499 wrote 740 MiB/s and worker 12398500 wrote 1,054 MiB/s during
an eight-second OS sample. Both had about 70% I/O PSI some avg10, 18% iowait,
4–5% CPU idle, and filesystem/page-cache blocked tasks. Worker 12398536 was also
writing about 1,001 MiB/s, but had lower pressure. CPU steal was 14.8% and 6.2%
on the two busiest workers; its contribution cannot be assigned to our code.

Gateway CPU idle was 28.9%, with CPU PSI some avg10 51.58%. Local fleet-list
response time rose from 93 ms to 1.023 s. Relay stats remained 9–17 ms. PostgreSQL
had no lock waiters, deadlocks, temporary spill bytes, or queued pool requests at
either snapshot. Its pool did record transient waiting and one cumulative request
error by the second sample; absence of a current queue does not exclude bursts.
The strongest observed bottleneck is worker filesystem/I/O contention, with
additional gateway CPU contention. PostgreSQL is not continuously saturated.

A five-minute heartbeat monitors until 13:25 UTC and reports meaningful changes.
Raw filtered observations: [first snapshot](monitor-1.txt),
[second snapshot](monitor-2.txt), [OS samples](monitor-os.txt).
