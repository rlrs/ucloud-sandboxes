# Isolate inventory write-lock work and coalesce routing IPC

The 0.5.107 realistic 512-agent run completed 4,096 cycles correctly but failed
latency qualification: measured p95 wake was 2.815 s and externally confirmed
tool execution 6.079 s. This run included bounded diagnostic profiles and a
45-second gateway GIL-switch-interval experiment, so it is diagnostic evidence,
not clean qualification. The interval was restored to 5 ms; no tuning is shipped.
The shorter interval did not show a benefit.

A 20-second gateway phase sample showed worker exec requests at 128 ms p95,
while whole exec handlers reached 2.258 s. Wake route commits and program park
projections reached approximately 2.14 s p95. The writer's separate eight-second
sample completed 543 operations with 281 ms of SQL transaction work and 660 ms
of FULL commits total. Those counters previously excluded BEGIN IMMEDIATE wait.
The gap therefore cannot be attributed to disk commit time alone. Profiles also
show full inventory/placement JSON decoding in the busy gateway interpreter.

Heartbeat inventory reconciliation now executes in the isolated routing writer.
Its ownership, generation, epoch, snapshot and absence checks use the same SQL
transaction; registry-reference readback/release remains with the caller.
This removes a writer-lock-holding inventory loop from the HTTP interpreter.
Input iterators are materialized before crossing the process boundary.

Concurrent routed writes share short IPC envelopes instead of one process-pool
submission/result notification per operation. An idle envelope waits at most
1 ms; an existing backlog is sent immediately, up to 32 commands at a time.
This is a transport batch, not admission control. Each command retains its own
FULL transaction, savepoint and durable acknowledgment. An individual conflict
does not fail unrelated commands. Process/IPC failure fails all unacknowledged
callers without replay, because some operations may already have committed.

An isolated 1,024-write benchmark with 64 callers and background JSON decoding
showed no throughput improvement from IPC batching alone (old 2.40 s, new
2.45–2.51 s). It is retained as a limitation; the loaded production replay must
justify the combined change. New `journal_batch_begin_wait_ms` telemetry separates
external SQLite writer contention from transaction and commit time. The load
harness also records the imported SDK version and client source digest.

Linux validation: 78 routing, real gateway lifecycle/heartbeat, durability and
wake-batch tests passed. Tests cover queued process loss, no acknowledgment
before commit, per-operation conflicts, iterator inputs and stale generations.
Native storage, gVisor, service dependencies and SDK are unchanged from 0.5.107.
