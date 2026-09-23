# Measured resident-wait selection

The existing memory-deficit policy now orders its candidates by observed benefit
instead of always selecting the oldest request. This adds no lifecycle state,
checkpoint deadline, or admission controller. Headroom still keeps every model
wait resident; only the existing memory/pressure decision authorizes selection.

Each sandbox incarnation retains up to 32 completed wait, park, and successful
wake measurements. The node retains at most 4,096 incarnation histories. A wait
ends when its model response starts waking it, excluding restore time. Deletion
discards history, cancelled waits are not completed observations, and a failed
restore is not recorded as a successful wake. Runtime generation remains part
of every key.

For measured candidates, the policy estimates remaining time from historical
waits that lasted longer than the current elapsed wait. It compares that time
with observed park plus wake cost. Transition estimates scale upward with the
actual cached cgroup footprint when the heap grows; configured memory limits do
not become either projected reclaimed bytes or measured cost. Profitable
candidates rank by expected resident byte-seconds per transition cost. Missing
history stays FIFO. Unprofitable candidates remain eligible when needed: this
ordering cannot deadlock real memory demand.

Ordering is cached for at most the existing 250 ms maintenance interval to
avoid sorting the full candidate set once per incoming request or memory sample.
Cancellation, retry eligibility, measured footprint credit, and the remaining
byte deficit are still checked on each admission.

## Advisory phases

The relay can attach an authenticated, generation/request-bound `resource_phase`
observation to its existing park request. The gateway forwards it only to workers
advertising `resource-phase-advice-v1`; older workers receive the unchanged park
request. The same strict transport validator runs at the gateway and worker.

Advice is scoped by opaque registration incarnation and monotonic sequence.
The worker converts its expiry to a local monotonic deadline once. A replay,
clock rollback, or older sequence cannot renew that deadline. New non-wait
phases invalidate old wait advice. Expiry also invalidates cached candidate
ordering immediately. Advice can replace the historical remaining-time
estimate, but never supplies transition cost, memory accounting, safe-point
permission, or lifecycle ownership. Without measured transition history the
candidate keeps the conservative FIFO fallback. Revoked registrations stop
producing advice at the relay; already-delivered advice expires locally.

## Transition budgets

Resident reclaim already subtracts in-flight projected releases from its byte
deficit, limits cache probes to measured clean file pages, and checks actual
MemAvailable again after completed work. Its maintenance executor has bounded
CPU parallelism and no unbounded backlog. These mechanisms should be retained;
another global park semaphore would hide the actual resource demand.

Startup/restore reservations are separate from resident reclaim. Their admitted
in-flight memory must be deducted from shared headroom, and checkpoint restore
bytes must come from durable measured artifact inventory rather than quota
limits. That transition-accounting extension is a separate implementation.

This candidate has policy/transport regression coverage. It is excluded from the
rc5 baseline and requires the subsequent real working-set density comparison
before promotion; the lightweight 512-agent run is not a density qualification.
