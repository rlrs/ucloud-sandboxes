# rc20 forced-wake attribution

Run `relay-load-6e95d7896a45` completed all 64 cycles and fleet-health checks on
four fresh workers. After excluding the first cycle, measured continuation p95
was 1.055 s and useful-exec p95 was 1.532 s. These improve on rc19's 1.221 s and
1.719 s respectively, but do not meet the subsecond continuation target. This
sequential production comparison is not a randomized causal experiment.

The six retained complete wake traces were matched uniquely to driver cycles.
They are a sampled diagnostic set, not a percentile estimate. Raw spans and
matches are in the adjacent `rc20-forced16-wake-*.json` files.

| Boundary | Six-trace range |
|---|---:|
| Gateway admission | 5–37 ms |
| Worker restore queue | 4–8 ms |
| Mount/validate/source preparation | 83–162 ms |
| Storage EnsureMounted server span | 34–63 ms |
| Native restore | 243–355 ms |
| Foreground artifact unlink | 30–57 ms |
| Warden total | 509–651 ms |
| Worker Python thread CPU | 42–59 ms |
| Continuation observation after gateway response | 169–383 ms |

Foreground artifact unlink now matches the complete artifact-cleanup phase to
within approximately 0.3 ms. Its own thread CPU is 17–26 ms. The retained sample
supports the intended removal of physical-retention cleanup from this foreground
boundary: rc19's broader cleanup region took 58–309 ms. It does not prove every
remaining millisecond is unlink itself, or that background physical cleanup has
zero contention cost.

Native restore and mount/validation remain the largest worker regions. For
sandbox 0014, cycle 2, trace `e42bcb477174b2cb0302e0409ccc2007`, native restore took
264 ms, validation 162 ms, runsc state 57 ms, resume 52 ms and unlink 57 ms. Warden
finished in 651 ms; the gateway wake completed in 712 ms. The continuation observer
arrived 242 ms later, giving 987 ms response-ready-to-observed-continuation.
Existing timing does not isolate native clone, process startup and kernel-state
restore, nor relay response transit from guest lazy page faults and the observer
round trip. Those are the next measurement boundaries if cold-wake latency is
pursued; attributing them all to Python or disk would be unsupported.

Gateway CPU admission was not the observed bottleneck in this small trial.
Distinct heartbeat CPU samples stayed at or below 17.5%, and sampled gateway
admission took 5–37 ms. The remaining CPU predicate for parked local wakes still
exists in source, but this test did not exercise it under pressure. No source,
configuration, worker profiler or additional workload was changed for this
analysis; only the existing driver report and retained Tempo spans were read.
