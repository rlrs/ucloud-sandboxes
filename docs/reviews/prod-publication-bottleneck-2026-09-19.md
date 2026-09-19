# Production latency diagnosis, September 19

The dominant measured delay in the 10:00–10:06 UTC workload was publishing
large sandbox snapshots through the registry, amplified by shared admission
queues and remote wake placement. The evidence does not justify blaming
Python execution speed or asyncio, or replacing the implementation language.
No runtime/configuration change was made during this diagnosis.

[Retained trace, heartbeat, migration and registry evidence](../benchmarks/prod-publication-bottleneck-2026-09-19.json).

## Measured critical path

Trace `cb8d63401e1526ed021c2532a12c975f` covers a successful response for
`super-park-harness-6bf558797f62`:

| Operation | Wall time | Calling thread CPU |
| --- | ---: | ---: |
| Relay response/wake delivery | 196.27 s | 4.03 s |
| Background snapshot publication | 132.20 s | 0.0045 s |
| Storage EnsurePublished RPC | 114.80 s | 2.44 s on server |
| General storage admission queue within that RPC | 17.67 s | waiting |
| Publication-specific queue | 27.10 s | 0.00017 s |
| Snapshot export/upload | 69.59 s | 2.36 s |
| Metadata GetVolume RPC | 17.39 s | 0.00070 s on server |
| Eventual sandbox restore | 9.54 s | 0.041 s |

Rows are nested/overlapping and must not be summed as independent phases.
The snapshot contained 725,598,208 bytes (692 MiB). The actual runsc restore
phase was 8.69 s. The long delivery interval contains repeated
`snapshot_publication_pending` admission responses before successful wake.

Worker 12396406 published 34 snapshots totalling 24,085,151,744 bytes
(22.43 GiB). Its publication gate reached four active plus three queued
publications. Its maximum recorded publication duration was 97.11 s and
publication-gate queue wait was 69.70 s. Worker 12396409 published only one
2.1-MiB snapshot in this window, illustrating how asymmetric the bulk work was.

Registry logs contain 2,923 PATCH requests and 32,801 GET requests during the
observed burst. One 87-chunk upload, ID
`01a0b91e-9e91-7aa5-b861-7e2b9c00d491`, spent 64.23 s inside registry PATCH
requests over a 69.21-second interval from first completed chunk to last.
Earlier similarly sized uploads completed their PATCH sequence in 11–13 s;
four later uploads took 61–69 s. Across all PATCHes, median/p95/max request
time was 0.113/0.774/4.474 s. The registry filesystem is
`/work/data/ucloud-sandboxes/live-ucloud-20260824a/registry/docker-registry`,
on the shared UCloud work mount. This places the dominant measured time in
the registry transfer/write path. It does not yet separate network receive,
filesystem wait, and registry process CPU.

## Why storage delays make control operations look stuck

`storage_native_daemon._observed_dispatch` gives every operation except
GetFeatures/GetMetrics the same eight-slot semaphore. EnsurePublished occupies
one of those slots while waiting on the separate four-slot publication gate
and while uploading. Metadata GetVolume and heartbeat ListVolumesPage requests
must queue behind that bulk work.

The measured GetVolume server spent 17.3868 of its 17.388-second lifetime
waiting for this semaphore. In trace `9bc2b8cf17a0598003806260771cd98`, a
6.056-second heartbeat spent 3.122 and 2.864 seconds waiting for two paginated
inventory reads; actual server thread CPU for those reads was 1.69 and
0.87 milliseconds. This is direct evidence of starvation through shared
admission, rather than expensive inventory computation.

The source worker's retained CPU sample was 90.506%, above the 90% dynamic
admission threshold. Its heartbeat then remained at 10:00:49 until recovering
around 10:03. Gateway wake placement requires a fresh, admissible source;
otherwise it seeks a remote destination and requires a portable snapshot.
Retained migration records show 20 completed `wake-*` migrations from
12396406 to 12396409, and no `consolidate-wake-*` migrations in this window.

The evidence supports an amplification loop: transient source pressure plus
metadata starvation delays fresh admission information; remote wake placement
then requires publication and further registry I/O. The exact initiating
predicate for each migration is not logged, so this causal explanation is
partly an inference from the measured queue waits, heartbeats and code.

## Python and profiling

Gateway/node HTTP handlers and the storage service are threaded synchronous
servers. The relay uses aiohttp/asyncio. The native storage exporter and runsc
also execute outside the Python request thread. Changing async frameworks or
rewriting the control plane would not remove the measured storage queues.

Low request-thread CPU is strong evidence of waiting in these spans, but does
not rule out CPU in a native exporter, another thread or the registry. Relay
thread CPU also includes unrelated coroutines during suspension. Both workers
showed CPU spikes during the workload; that does not establish the CPU source.

The workload had finished by live inspection (zero routes/pending requests at
10:06:13), so no CPU/off-CPU sampling profile of the burst was captured. The
next useful profile is a representative large-snapshot burst, measuring:

1. Registry PATCH/GET latency, filesystem and network throughput, block-I/O
   wait, registry Go CPU, and exporter CPU on a common timeline.
2. Storage metadata queue latency separately from publication and lifecycle
   admission, including heartbeat age and local/remote wake decisions.
3. Python GIL/thread and relay event-loop lag as secondary checks, rather than
   inferring them from an overall slow HTTP request.

The first design changes to evaluate are isolating bounded metadata admission
from publication, keeping remote-upload waits out of shared lifecycle slots,
and preferring bounded local-wake waiting over an expensive migration caused
by transient CPU pressure. Registry data placement/throughput is the next
storage optimization candidate; do not move it or raise all concurrency limits
without measurement and a durability-preserving migration plan.
