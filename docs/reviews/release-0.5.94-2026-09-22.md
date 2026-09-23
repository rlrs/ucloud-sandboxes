# Local resource checks for retained relay parks

At 256 concurrent managed agents, the .93 gateway spent substantial work
repeating warm-retention park attempts every three seconds. Workers now retain
an optimization-only local pending entry and check pressure every 250 ms,
without registry access while memory headroom remains sufficient. The relay
keeps the durable intent and retries after 30 seconds as a restart backstop.
No client admission limit or wake concurrency limit is added.

Every actual park still passes the current generation and durable wake fences
under the existing lifecycle coordinator. A response cancels its pending park;
a deleted/replaced generation cannot be parked by the old entry. Transient
checkpoint failures retain pending work and back off. Idle-parking settings do
not disable these explicit relay park intents. Already-parked snapshots skip
warm retention and can acknowledge the durable retry immediately.

A deferred park also persists the gateway's original transport epoch in
PostgreSQL, without claiming that a checkpoint completed. If pressure triggers
local parking and a migration follows before the retry, the eventual wake can
still compare transport epochs and force the correct reattachment.

Linux runtime/pressure/admission/relay tests passed (95, one pre-existing skip).
Real PostgreSQL tests and the next full mixed-load result are recorded alongside
this review. No subsecond or overall qualification claim is made yet.

The full mixed run completed 2,048 correct cycles. Before profiling, a
20-second phase sample showed 176 park calls against 173 wakes (previously
roughly 548 against 100). Worker wake p95 remained 111 ms; gateway wake p95
was 3.00 s, with each routing write waiting roughly 0.75–1.1 s. The target
was already missed before CPU profiling.

The 15-second py-spy attachment at 100 Hz introduced heavy interference
and fell behind its own sampling schedule. Its 976 samples are useful for
locating CPU work (423 samples in fleet-list rendering), but the resulting
full-run latency distribution is NOT a valid release performance comparison.
The recorded measured-cycle p95 was 22.99 s, median 4.04 s; long queued tails
persisted after attachment. The next qualification must run without a profiler.
