# Cold published sandbox offload

The autoscaler can now reclaim worker disk reservations from already-published,
parked sandboxes without stopping the worker or generating another checkpoint.
It invokes the existing gateway detach protocol; there is no second eviction or
restore implementation.

The policy starts when measured storage hard reservations reach 90% of capacity,
and selects enough claims to target 80%. It can also act below that threshold
when a queued create needs more disk than the node's remaining hard headroom.
For the latter case it requires enough eligible claims, within the existing
per-cycle detach budget, to make the request fit. Oversized requests are not
made schedulable by evicting unrelated work.

Candidates rank by released reservation divided by published layer bytes, with
an age preference. Published layer bytes estimate eventual remote restore cost;
this is not measured restore time or a promise of physical bytes reclaimed.
The action does not initiate uploads. Active model waits, ready deliveries,
pending wakes, deletes, unavailable workers, and workers already stopping are
excluded. Missing disk measurements cannot initiate offload. Interrupted,
already-committed detach intents remain eligible for completion.

Automatic requests carry an `if_cold` observation containing the exact generation,
create operation, spec hash, node/job, node epoch, activity epoch and snapshot
digest. The routing writer transaction rechecks that observation, current parked
state, pending wake demand and nonterminal program requests before committing
`detaching`. A wake or new model request which won before that transaction blocks
offload. A later wake follows the existing detaching/detached restore path.
Explicit user or drain detach requests retain their existing empty payload.

The existing gateway verifies the portable descriptor and retains its registry
reference before fencing eviction. Worker deletion releases native hard claims;
only committed detach removes the gateway's local reservation. A timeout stays
`detaching` for safe replay. The plan itself never credits capacity: subsequent
heartbeats and committed route state expose the actual release. The autoscaler
reports `coldOffloadPlan` and actions in `storage_native_detach_results`.

Validation covers cost ranking, sufficient-space selection below the pressure
threshold, expense budgets, active-wait exclusion, unknown evidence, interrupted
detach recovery, exact snapshot/epoch fences, pending wake and program-state
transaction predicates, and a new model request arriving between selection and
gateway detach. Existing gateway tests cover ambiguous eviction, verified
publication before eviction, and detached wake without the former worker.

This does not offload resident or unpublished model waits, alter storage-format
journals, reduce disk guarantees, or establish a measured production density gain.
