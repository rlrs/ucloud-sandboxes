# Avoid inventory copies on active exec polls

Exec polling previously copied an entire worker inventory before checking its
active count. Read just the header first, then fetch the inventory only when
the worker reports no active sandboxes. Parked sandbox inventory still prevents
a false stale-route result. Filesystem identity checks also move outside the
control-state connection-pool lock so slow stat calls cannot block readers
returning their connections. Identity validation remains mandatory.

The fleet helper captures database file identities in its parent and checks
them before opening stores and around reads. A child restart after removal or
replacement fails closed instead of recreating an absent database.

100 Linux control-state/gateway/registry tests and 9 fleet-reader/control-state
tests passed, including slow-stat concurrency, active and parked exec routes,
and helper crash followed by database removal or replacement. Full production
load qualification remains required.

## Load results and rejected experiments

The 256 × 8 diagnostic run completed 2,048 correct cycles but failed latency:
p95 wake 1.441s, response-ready to usable tool 2.735s. This run included two
short method-timing windows and a 45s switch-interval experiment, so it is not
a clean performance qualification. Python's 1ms thread switch interval did not
improve the measured gateway bookkeeping and was restored to 5ms.

An uninstrumented, forced-park 64 × 4 barrier run completed 256 correct cycles.
All 192 measured cycles observed a real park. Wake p95 was 1.027s and usable
tool p95 1.371s, also a qualification failure. No client or cleanup errors.

Isolating hot routing writes in a separate process did not improve a concurrent
HTTP/fleet-poll microbenchmark, on either the Linux test host or the production
gateway while idle. A batched variant also showed no meaningful improvement.
Neither architecture change was adopted.
