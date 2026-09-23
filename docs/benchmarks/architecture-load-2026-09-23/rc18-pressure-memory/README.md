# rc18 pressure trial evidence

Run `relay-load-ca1cbcf81ed1` exercised the same 256-primary pressure profile on
four rc18 workers. It completed 502 cycles before failing on sandbox 0160's
managed-agent startup deadline at 15:14:54 UTC. The report finished at 15:16:27;
cleanup recorded no errors. Fleet health failed with 28 resource/heartbeat
failures. This is **not** a successful pressure or performance qualification.

The pretrial workers were quiet: I/O full PSI was zero on three workers and 1.12%
on the fourth. Under load, captures carried measured byte estimates: an early
worker snapshot showed ten captures with 16.66 GB projected reclaim, rather than
the previous trial's 17 captures with only 3.33 GB projected. The current target
may shrink after dispatch, so in-flight bytes can exceed a later target without
new admission. By 15:15:50, observed completed-park counters totaled 136; current
captures were two, three, two, and two across the workers.

The retained worker journals contain no `runsc checkpoint` command timeouts in
the captured interval, unlike rc17. This supports improved capture progress,
but does not prove every operation met its deadline. Several heartbeat samples
became stale and I/O full PSI exceeded 50%. Worker 622 recorded a 2.585-second
clocksource watchdog interval and worker 626 a 3.400-second interval; those timing
gaps do not establish the component responsible for the stalls.

At 15:15:50 the durable managed-process records showed 223 running primaries and
no terminal failures. Two-second samples through 15:16:14–17 showed minimum
physical available memory of 8.36–11.31 GiB across the workers, and minimum
RAM-backing available space of 10.48–13.31 GiB. No sampler failed. These periodic
observations cannot prove the instantaneous headroom at every allocation.

The raw RAM samples, observations, kernel timing messages, and derived summary
are retained here. Full worker logs remain in the private qualification capture
`/private/tmp/rc18-growth-evidence`. Collection was read-only; rc18 source remained
unchanged throughout the run.
