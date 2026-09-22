# Release 0.5.78: gateway contention fix

Runtime commit `a1a94b20ea5a913dba5eb4c4aed2348ea7e8571f`.

## Evidence and change

The preceding 0.5.77 natural 256-agent run completed correctly but regressed to
8.656 s wake p95. A Python GIL profile of the gateway found `Condition.notify`
was the largest self-time entry (128/693 samples); SQLite transaction admission
repeatedly broadcast to writers waiting for a closed batch. Those writers then
broadcast again, contending with the flusher that could release them. The same
profile showed owner-route decoding for warm wake shadow planning.

The batch writer now signals its flusher separately. Writers are notified after
commit or abort. Per-operation savepoints, commit acknowledgements, database
identity validation and ownership fences are unchanged. Warm or already-waking
sandboxes skip the observational placement simulation, while their actual fenced
wake still reaches the owner. Expected `park_deferred` responses no longer cause
another durable program error write.

On Linux, 128 concurrent writers performing 2,048 FULL-synchronous SQLite writes
completed in 0.403 s versus 0.518 s, with process CPU 0.220 s versus 0.440 s. This
small benchmark measures contention, not production wake latency. Its script,
previous writer implementation and results are retained with the artifacts.

The CPU-profiled production run is not a latency baseline: sampling caused
substantial overhead. Production comparison runs below do not run a profiler.

## Validation and deployment

The full Linux suite with real PostgreSQL passed: 1,205 tests, 10 skipped,
102.958 s. An earlier run failed one test which manually signalled the previous
condition variable; its synchronization was updated and the full run repeated.
Durability, rollback isolation, concurrent progress and warm-wake behavior have
regression coverage. Ruff and diff checks passed. The exact wheel passed the
installed-package verifier in a clean Linux venv. Both future worker bundles
validated on the production worker kernel 7.0.0-30-generic.

Gateway/relay installed at 12:43:48 UTC. All 110 package files matched the wheel.
PostgreSQL authority and its SQLite cutover fence remained in place, with backup
before restart. Workers 12398499, 12398500, 12398539, 12398541 and 12397503 all verified 0.5.78;
autoscaling resumed afterward. Public gateway and relay health checks returned
0.5.78. The preserved unrouted sandbox on draining worker 12398541 was not deleted.
No SDK or Verifiers update is required.

## Production results

The natural 256×8 run completed all 2,048 cycles correctly. Measured wake p95
improved from 8.656 s to **3.773 s**, and response-ready-to-verified-exec p95
improved from 10.517 s to **4.694 s**. Both still exceeded the prior baseline
and the 0.8 s target, so optimization continued rather than accepting this as
finished.

The forced 256×3 run completed 768/768 cycles. Wake p95 was 13.494 s, but a short
5 Hz gateway stack sample was taken during this run; treat it as diagnostic,
not a clean latency comparison. Traces of warm wakes showed only 4–25 ms on the
worker while gateway request completion took seconds.

An isolated routing benchmark on the production gateway reproduced contention:
128 concurrent writers, 512 logical requests × four durable program transitions,
p95 1.641 s, CPU 2.853 s. With a 5 ms routing coalescing window the same workload
measured p95 0.473 s, CPU 1.425 s. Release 0.5.79 applies that measured setting.
