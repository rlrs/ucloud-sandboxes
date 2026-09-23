# Restrict relay loss reconciliation to outstanding callers

A loaded relay CPU profile found repeated reconstruction of retained terminal
sandbox history. The existing PostgreSQL reconciliation already limited writes
to outstanding requests, but its SQLite input still scanned all seven days of
terminal program requests and node losses on every maintenance pass.

Both relay backends now identify the exact sandbox incarnations with outstanding
work or an undelivered committed response before asking the routing store for
loss/deletion proofs. An empty set does no SQLite lookup. Bounded batches probe
the existing incarnation indexes; they do not limit concurrent relay work. The
routing query explicitly selects the sandbox index because SQLite otherwise
chose the terminal-state/time index and amplified work for correlated probes.
A read-only production-data benchmark caught that plan before deployment.

Reconciliation still rechecks outstanding requests under the existing locks.
A request arriving after the candidate snapshot is considered next pass. The
seven-day retention, generation matching, loss precedence, durable model results,
and ownership rules are unchanged. The no-argument routing query remains available
for diagnostics, with duplicate incarnations collapsed inside SQL.

On retained production state, selecting 512 known incarnations took 4–8 ms
versus 274–298 ms to reconstruct 33,213 terminal incarnations. The targeted
results exactly matched the corresponding full-history subset. This is a
component improvement, not qualification of 512-agent wake latency.

Linux validation covers real PostgreSQL, the legacy relay, routing, maintenance
with empty/nonempty candidate sets, retained model results, old generations,
expiry and multi-batch lookups. The load harness now records the failing stage
and cycle so a timeout cannot hide whether claim, wake, dispatch or tool wait
failed. No SDK or native storage format changes are required.
