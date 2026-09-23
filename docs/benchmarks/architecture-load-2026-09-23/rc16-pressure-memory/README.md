# rc16 pressure trial memory and liveness evidence

Run `relay-load-cdf004f3586a` began at 14:01:46 UTC on September 23. It requested
256 managed primaries with 1.5 GiB heaps and 2 GiB memory limits. The run stopped
after three completed cycles because a transient managed-process status read
timeout was returned as HTTP 409. Cleanup completed without recorded cleanup
errors at 14:05:52 UTC. This trial did **not** pass correctness or fleet health.

The retained two-second samples cover startup through part of cleanup
(14:01:35–14:04:23 UTC). Minimum physical available memory was 29.44–41.81 GiB
across the workers; minimum RAM-backing available space was 31.78–43.93 GiB.
There were no sampler errors. These are instantaneous samples, not allocation-time
proof. At 14:04:21 UTC the durable managed-process records contained 124 running
primaries and no signaled, failed, or nonzero-exited records for this run.

The fleet-health gate recorded a failed gateway resource probe affecting all four
used workers at 14:03:01 UTC, followed by a stale worker 12400587 heartbeat at
14:03:19. A separate later observation found worker 12400589's heartbeat stale
while its SSH RAM sampler continued. Thus memory headroom alone did not imply a
healthy control path. Worker journals included delayed HTTP responses ending in
broken pipes; those errors identify disconnected callers, not the cause of delay.

Worker 12400588's kernel recorded a 5.629786523-second clocksource watchdog readout
interval at 14:02:45 UTC (`kernel-evidence.txt`). This establishes a timing gap but
does not identify a host, guest, storage, or scheduling cause. The retained kernel
journals had no OOM-kill or block-I/O-error matches during the inspected interval.

A bounded read-only snapshot of worker 12400589 at 14:06:12 UTC, after cleanup,
found its node-agent alive with 97 threads sleeping in futex, poll, or timer waits
and no threads in uninterruptible D state. Memory PSI was zero; I/O full PSI was
12.83% over 10 seconds and 51.23% over 60 seconds. This shows the earlier stall had
cleared; it cannot establish which application lock or operation caused it.

The raw RAM samples and derived summary are retained here. Full service/kernel
journals and the bounded process snapshot were retained privately under
`/private/tmp/rc16-growth-evidence` during qualification. No source or deployment
change was made while collecting this evidence.
