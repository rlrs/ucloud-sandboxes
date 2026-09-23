# Commit wake confirmation and program outcome together

Warm and restored wakes previously queued route confirmation and program
completion as separate SQLite writes. They now share one FULL transaction.
The existing owner, generation, boot epoch and activity revision checks run
inside that transaction. A stale proof cannot publish an acting program, and
a program write failure rolls back the route update instead of acknowledging
partial progress. Implicit wakes use the same confirmation path without a
program projection. Snapshot-reference retirement still happens after commit
and uses readback after an ambiguous error.

161 Linux routing/gateway tests pass, including injected program-write failure,
stale worker proof, externally visible committed state and exact commit count.
The existing lost-commit-acknowledgment tests now inject at the outer combined
transaction boundary; they continue covering both pre-commit and post-commit
failures and the corresponding snapshot-reference cleanup.

This reduces durable operations without relaxing durability or lifecycle
fences. The full load tests remain the performance acceptance criterion.

Clean mixed 256 × 8 run: 2,048 correct cycles; measured p95 wake 2.137s and
response-ready to usable tool 4.762s. This fails qualification and is worse
than the preceding run. All 256 sandboxes were placed on two existing workers
(127/129), while new workers became available later. The busy workers showed
18–20% full I/O stalls with about 20% CPU use. Different initial fleet capacity
means the change's independent effect cannot be inferred from this comparison.
