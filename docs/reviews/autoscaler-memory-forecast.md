# Autoscaler memory forecast

The autoscaler forecasts near-term resident memory rather than permanently
reserving each attached sandbox's configured memory limit. Parking remains a
capacity-sharing mechanism; neither 256 sandboxes nor the sum of parked limits
is an admission ceiling.

The forecast combines fresh host working memory, outstanding create/wake
reservations, and concurrent queued or prepared requests. Actual host working
memory includes mapped, file-backed guest pages even when Linux reports those
pages as reclaimable. Completed startup reservations give way to measurements;
parked owners do not retain a full declared-memory charge. A partially completed
transition can temporarily overlap its measured use and remaining full promise,
which favors earlier provisioning without rejecting work.

The existing 80% utilization target supplies headroom. Supply uses actual worker
RAM when available and credits provisioning workers once. Scale-down uses the
same forecast and capacity units, along with the existing pressure cooldown and
idle/drain checks. Missing observations are not evidence that an occupied worker
has spare memory.

Sustained pressure can request another bounded provisioning wave while a worker
is booting. Startup headroom is relative to the ready fleet, so a busy fleet
cannot suppress scale-out merely because its configured minimum is zero.
Provider purchase limits still apply. The production policy permits zero to
eight workers, with at most two new workers per reconciliation cycle.

Optional wake consolidation includes file-backed working memory and refuses a
destination already exceeding the configured I/O pressure threshold. Healthy
consolidation remains enabled. Existing create, wake and execution admission is
unchanged; this forecast is a scaling decision, not an additional worker limit.

Adding workers cannot immediately redistribute already-running guests. The
earlier pending-work forecast is intended to provide capacity before severe
pressure; existing placement and parked-wake relocation can then use it. This
does not establish a latency guarantee at any particular sandbox count.
