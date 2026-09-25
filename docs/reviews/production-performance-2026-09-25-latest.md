# Latest retained production run: September 25

Read-only investigation at 05:35 UTC. Production is idle on rc29, with no
sandboxes, workers or pending relay deliveries and a 9 ms health response. No
production configuration, service or workload was changed.

The retained workload observations are around 00:33–01:00 UTC. All 1,003 sampled
heartbeats during that interval report rc29. There were ten sandbox workers and
four builders. The builders' zero sandbox counts are not unused sandbox capacity.

## Results

22,854 distinct completed wakes: median 0.196 seconds, p95 4.105 seconds, maximum
198.328 seconds. Response-ready to completed-wake p95 is 4.191 seconds. These
are completed operations, not proof that every original request succeeded.

### 1. Placement serialization remains the primary gateway target

Trace e97607601ccf3f49015a219f48fc265f records a 40.900-second wake reservation:
32.109 seconds waiting for the process lock, 0.010 seconds for the file lock,
and 8.780 seconds holding the lock. Thread CPU was only 0.0065 seconds.
Trace 3181860efbab7bb404155c86927656e5 spends 9.850 seconds waiting and only
0.0166 seconds holding the lock. Several create traces time out at 30 seconds
inside placement and return HTTP 503. This is real serialized waiting, not
30 seconds of placement computation.

The routing writer queues reservation writes with exec, program-transition and
inventory writes. `_dispatch_many` executes each command with its own durable
transaction and returns the complete envelope before acknowledging any caller.
A placement lock holder can therefore wait behind unrelated writes and also
wait for later commands in its envelope. This is an identified amplification
mechanism; the retained traces do not isolate how much of the 8.780 seconds was
writer queueing, SQLite contention, commit I/O or other reads.

Next fix: measure queue/transaction/ack latency and remove whole-envelope
acknowledgment delay for critical reservations, preserving durable owner and
capacity fences. Move expensive preparatory reads outside the reservation where
safe, revalidating the decision at commit. Do not merely extend the 30-second
admission timeout or remove serialization without a replacement capacity fence.

### 2. Growth accounting is driving expensive reclaim despite available RAM

Workers held roughly 45–51 sandboxes each and generally had median 66–68 GiB
MemAvailable. Worker 12402334 at 00:34:31 reported 68,711 MiB available, 68.94 GB
admitted physical demand, no pending demand, reason `queued_demand`, and a
3.95 GB reclaim target. Its RAM backing still had 80.96 GB available. These
'admitted' bytes include growth forecasts; they are not all active copy jobs.
216 heartbeat samples report queued_demand; 492 report resident_headroom.

`_refresh_growth_forecasts_locked` includes active growth intents;
`_growth_remaining` charges the bound minus observed resident/shared pages.
Audit forecast lifetime, observation freshness and safe-wait transitions before
reducing the accounting: these reservations also protect against SIGBUS/OOM.
The intended improvement is charging the remaining real growth obligation
accurately and avoiding premature hibernation, not discarding backing admission.

Checkpoint traces show 12.10, 27.27 and 57.29 seconds in runsc checkpoint, which
accounts for nearly all their park time. Trace 6824ff3f10f87e1a0ed8a74032f29403
shows a 15.47-second gateway wake, including 3.02 seconds of worker lifecycle
wait and 7.30 seconds in worker wake. Worker I/O pressure is material (some
p95 values 20–28%), but CPU/RAM exhaustion is not a sufficient explanation.

### 3. Superseded lifecycle work still creates noise and contention

Recorded transition errors: 579 distinct requests with park superseded by wake;
310 requests with lifecycle-busy errors (316 events); 23 unsafe-park/active-I/O
conflicts; 12 growth waits superseded by wake; eight node timeouts; five memory
headroom reservation errors. These are internal transition records, not counts
of terminal client failures. Completed wake traces show that at least some busy
operations recover.

Next fix: treat intentional supersession as a lifecycle outcome and release its
claims promptly, while preserving late transport receipts. Examine the 23
active-I/O conflicts separately rather than treating every 409 as harmless.

### 4. Repair the new relay tracing

The rc29 `relay.lifecycle.dispatch` instrumentation calls the global
`trace.get_tracer`, but Telemetry creates a private TracerProvider and the relay
state never receives its tracer. No dispatch spans were found. Wire the existing
Telemetry instance into the durable dispatcher and test exporter output; do not
introduce a second tracing configuration. This is a flaw in the latest
instrumentation, so committed-response queue age cannot yet be attributed from
those spans.

The 00:50 five-minute histogram estimates (~58-second placement p95,
~100-second checkpoint p95) corroborate long tails but are bucket estimates for
particular operation populations, not exact whole-run wake percentiles.
`relay.wait_for_worker` includes inference and must not be called wake-only delay.
One metrics event was dropped due to SQLite busy. The exact cause of the external
run ending cannot be established from these backend transition records alone.
