# Reduce gateway transaction and upload-close overhead

The stronger 0.5.109 test includes a checksummed 64 KiB tool upload after each
model response. All 4,096 cycles at 512 agents completed correctly, with no
cleanup failures, but measured wake p95 was 1.623 s and time through uploaded
tool execution was 5.677 s. The run included bounded diagnostic profiles and is
not clean qualification. All 256 forced park/restore cycles at 64 agents passed
correctness: wake p95 0.752 s, uploaded tool p95 1.332 s. The overall target is
not achieved.

At 512 agents, the four-vCPU gateway was about 81% busy; its HTTP interpreter
used 1.12 cores, relay 0.76, fleet reader 0.14 and routing writer 0.09. Workers
continued to execute tools quickly. An eight-second routing-writer sample spent
2.14 s waiting to acquire SQLite's writer lock, versus 210 ms in transactions
and 323 ms committing. Startup route writes still ran in the HTTP interpreter.

This release moves create allocation, create confirmation (`upsert_sandbox`),
and batched wake reservations to the same isolated routing writer as heartbeat
reconciliation and wake/exec acknowledgment. The caller retains placement locks
until durable acknowledgment; the child rechecks generations and ownership in
the original SQL transactions. No uncertain operation is replayed.

Heartbeat identity checks now read distinct owner tuples instead of parsing
full specs and checkpoint descriptors for every matching sandbox. Matches on
any node ID, job ID or URL still return every conflicting tuple. There is no
TTL cache, so external changes remain visible on the next read.

A successfully consumed streaming upload now marks the framed request body
consumed, avoiding the rejected-body socket drain path. Partial uploads still
close and drain under the existing bounded rejection protocol.

Linux validation: 39 tests passed across routing writer failure/durability,
real HTTP gateway creation and lifecycle, wake admission, identity projection,
and streaming uploads. Tests cover committed route visibility before dispatch,
recreated generations rejecting stale wakes, distinct conflicting identities,
and full versus truncated bodies.

Native storage and runtime dependencies are unchanged. The role-aware idle
worker rollout also handles builders without a storage service; all six workers
and both recovered builders were verified on 0.5.109 before these changes.

A 256-agent diagnostic alternated the default 5 ms interpreter switch interval
with 0.1 ms twice. Interior 20-second windows had tool p95 1.186/1.189 s at the
default and 4.443/4.222 s at 0.1 ms. The shorter interval is rejected. A later
20 ms comparison overlapped candidate packaging in its early windows and is
not qualification; it provided no reason to change the default. Both probes
verified restoration to 5 ms. This diagnostic completed all 4,096 cycles
correctly, but its combined percentile deliberately includes altered settings
and must not be compared as a release result. Raw reports and timing windows
are retained.
