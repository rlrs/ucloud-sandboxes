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
