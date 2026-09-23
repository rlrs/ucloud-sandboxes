# Candidate 12 pressure correctness failure

Run `relay-load-398dd9b5be64` began at 13:03:43 UTC on 23 September 2026 with
256 sandboxes, 1.5GiB resident memory per agent, 384MiB rotating dirty pages,
2GiB guest memory limits and 20–25-second model waits. It completed 66 cycles
before reporting sandbox `0199`'s managed agent had exited. Cleanup finished at
13:05:30 UTC. This is a failed correctness qualification, not a latency result.

The gateway's retained managed-process records provide stronger evidence than
the original empty-stderr error: **16 agents terminated with SIGBUS (signal 7),
exit code -1, and no stderr**. Three generation-2 agents (`0197`, `0198`, `0199`)
on worker `12400421` terminated within 19ms at 13:04:49 UTC. Thirteen generation-1
agents on worker `12400419` terminated at 13:05:01–03 UTC. Exact terminal records
and placement identities are retained beside this note. These records came from
the guest supervisor's wait status and survived sandbox route cleanup.

Bounded service and kernel journals from all four workers were collected before
worker replacement. Neither affected worker's kernel journal contained an OOM
kill or segfault. SIGBUS points toward mapped-memory backing faults, but these
records alone do not prove tmpfs exhaustion or distinguish it from another
backing failure. The last retained fresh resource observations for the affected
workers were 13:04:08 and 13:04:17 UTC, too early to establish their exact backing
capacity at failure. Runtime journals/files had already been removed by cleanup.

Follow-up must measure the active-memory tmpfs's own `f_bavail` and physical host
memory independently. Free host RAM is not proof that a bounded tmpfs can accept
more mapped guest pages. Candidate 14 adds a shared typed backing-capacity
observation to admission and resident parking policy; configured but unavailable
evidence remains unknown. Peak statvfs collection and a successful repeated
pressure workload are still required before declaring this failure resolved.
