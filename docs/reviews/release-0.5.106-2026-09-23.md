# Queue tools across lifecycle transitions and forecast cold-burst memory

A 160-agent rolling reproduction on 0.5.105 placed 150 guests onto its first
worker before later capacity arrived. The workload used 512 MiB of resident
memory and dirtied 128 MiB per cycle, with 24 concurrent fleet pollers. It failed
when a tool request raced a park transition and received HTTP 400. Completed
measured cycles had 4.874 s p95 wake and 5.823 s p95 externally confirmed tool
latency. Cleanup succeeded. This is failed qualification, not an acceptable
operating point.

A trace captured the onset of the storage/memory stall. Compaction was dominated
by XFS mount/log recovery (140 events), process creation and network setup. The
backing-file speculative-read stack addressed in 0.5.104 did not appear in this
sample. During cold startup, the worker wrote 2,349 MiB/s and spent 62% CPU time
in the kernel. Raw load results, stack samples and host counters are retained in
`../benchmarks/cold-density-2026-09-23/`.

Tools now join an existing lifecycle transition before taking shared activity
ownership, reading registration and ensuring the current generation is running.
This preserves park/delete exclusion. If the bounded admission wait expires,
no command has launched: the gateway receives the existing retryable exec
admission result. Deletion or generation changes during the wait are rechecked.
The change applies to the direct runtime; legacy coordinator callers retain
fail-fast semantics unless they explicitly request a wait.

Capacity preparations previously collapsed the entire burst's CPU and memory
into one reusable request, while summing disk. A cold burst could consequently
prepare one worker and only request more after it was already under pressure.
Preparations now forecast their concurrent memory footprint against the existing
memory-utilization target. A single request's forecast never exceeds one full
node. As preparations are consumed, observed working-set memory (including
file-backed guest RAM) accounts for already running guests. This changes scale
planning only: it adds no create, wake or execution limit and does not reserve
all resident sandboxes' nominal memory. CPU remains reusable. Consumed/expired
preparations cease contributing forecast demand.

Linux validation: 78 policy, lifecycle, exec and node-agent tests passed,
including transition overlap, deletion during the wait, retryable pre-exec
expiry, cold-burst planning and observed working-set replacement. Native storage,
gVisor, dependencies and SDK are unchanged. Production load qualification is
still required; neither change establishes the sub-second target on its own.

The subsequent cold 256-agent run completed all 2,048 cycles without correctness,
health or cleanup failures. Placement was 83/72/57/44 guests on four workers,
with no compaction in a mid-run sample. Measured wake p95 was 0.356 s and
externally confirmed tool p95 was 0.818 s. The provisioning-overlap tool phase
still had p95 1.234 s, so the strict overall qualification remains failed.
This demonstrates the burst-planning improvement without claiming completion.
