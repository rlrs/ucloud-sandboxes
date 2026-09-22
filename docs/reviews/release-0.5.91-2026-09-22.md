# Create confirmation fencing and continued wake qualification

The 0.5.90 cold-fleet mixed-load run failed after 147 completed cycles. Sandbox
`relay-load-e0436b0cb7c5-0194` created successfully on worker 12399484 and registered
with the relay, then its first upload received `sandbox route not found`. The
worker retained the sandbox after gateway cleanup; it was removed by exact ID and
generation. No customer sandboxes were involved.

The matching reconciliation race is deterministic: create confirmation inherited
the placement heartbeat's old activity revision. A complete inventory sampled
before creation, received later, could delete the confirmed running route. The
receipt-time fence does not establish sampling order. Node create and targeted
recovery responses now carry a post-observation activity revision and boot epoch;
the gateway persists that fence. An older inventory cannot remove the route,
while a genuinely newer absent inventory still can. A different boot is rejected.
An inventory-removal metric now records the two revisions for future diagnosis.

The regression fails with the former confirmation behavior and passes with the
fix. 232 Linux tests pass, including routing, gateway, node runtime, real HTTP
create/recovery, and streaming upload. Two stale test assumptions were corrected:
a private registry fixture directory must be mode 0700, and adaptive park retry
intervals need not exceed ten seconds.

The load harness now separates first usable tool execution from its subsequent
512 MiB memory integrity scan. It still performs the same scan and file check on
every cycle, and any failure invalidates the run. First-tool latency includes an
external exec confirmation; full integrity completion is reported separately.
Earlier combined latency results are not directly comparable to this new metric.
Explicit forced-parking qualification is also required: naturally retained warm
sandboxes cannot qualify actual restore performance.

Qualification is still open. No subsecond production claim follows from these
component tests or the aborted load run.

The subsequent cold-fleet mixed run completed all 2,048 cycles, with no route,
health, polling, integrity or cleanup failures. It still failed latency:
post-warmup wake p95 3.402 s, first usable tool p95 9.512 s. Most cycles stayed
warm; this does not qualify actual parked restores. Stack sampling and brief
component tests on the driver occurred after it was already above target, so the
run is diagnostic rather than a clean performance comparison. Guest tool median
was about 75 ms in a late 200-cycle sample; most observed latency was outside it.

Release 0.5.92 changes fleet reads to one SQLite JSON aggregate rather than a
Python sqlite3 cursor step per sandbox, reducing GIL handoffs during overlapping
placement/list scans. Returned routes remain fresh independent objects; malformed
JSON still fails closed. Listing also avoids checking each inventory entry when
an active worker already disproves the empty-worker absence predicate.

On the idle production gateway's Python 3.14.4, the contention benchmark elapsed
time fell from 10.20 to 5.16 s and write p95 from 0.699 to 0.337 s. Worst write
latency increased to 3.38 s, so these figures are not a qualification pass. An
isolated Python 3.13.2 run was slower (11.60 / 5.70 s); no interpreter change was
made. Linux routing/control tests passed (155 before the final assertion and
predicate adjustment, 95 afterward). Next: full live qualification and explicit
parked restore coverage.
