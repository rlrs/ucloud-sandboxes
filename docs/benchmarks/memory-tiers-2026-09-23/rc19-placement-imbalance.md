# Create placement under a startup CPU wave

The rc19 pressure run ended with 55, 64, 73, 64 sandbox placements on workers
12400713–12400716. The initial wave was balanced: heartbeats at 16:59:41 UTC
reported 35, 36, 37, 38 owners. At that point three workers advertised 97–98% CPU,
while worker 12400715 advertised 84%. Those CPU samples remained cached for about
20 seconds. The gateway's eligibility filter excluded the three workers before
its assigned-shape ranking ran. Worker12400715 grew to 54 and then 72 observed
owners; its final assigned count was 73.

The retained creation traces corroborate the second part of this mechanism:

- `0145` selected 12400713 at 16:59:35, received explicit
  `node_active_admission_deferred`, and selected 12400715 at 16:59:36.
- `0164`, trace `b712b0fc6e570e90dd5a43e3d9df0cd`, was rejected by 12400713 and
  12400716 in sequence, then succeeded on 12400715 at 16:59:39. Both rejections
  took about 850 ms, consistent with the bounded local CPU retry path. The
  retained response contains the generic admission error, so the exact local
  predicate is inferred from timing and concurrent CPU metrics, not an error
  body naming CPU.
- Sampled creates 0169, 0170, 0175 and 0180 then selected 12400715 during the cached
  exclusion window. Image cache hits were present on the rejected workers and
  the eventual destination. Image locality does not explain those moves.

Authoritative placement reservations were not missing. Selection and durable
route allocation run under the gateway placement lock, and assigned-shape
pressure counts completed and pending routes. But that score cannot choose a
node removed by a cached CPU filter. The subsequent local CPU retry/reselection
path can also concentrate work even if the gateway filter is relaxed alone.

## Offline correction

Create placement and its pre-dispatch recheck now retain CPU utilization as a
relative ranking signal rather than an eligibility cutoff. Durable assigned
shape is the primary balancing term; live pressure and in-flight operations
break comparable assignments, then image locality. Hard physical shape,
available disk, storage constraints, capabilities, readiness and memory checks
remain unchanged. Existing gateway wake/migration paths retain their previous
policy; this is a bounded create-placement correction.

At the worker, startup/restore `_reserve_active_capacity` also treats sampled
CPU as advisory. Existing startup/restore queues still bound operation fanout,
and per-sandbox native cgroups schedule CPU. TransitionLedger physical-memory
and RAM-backing claims, owner/drain fences, shape limits and wait deadlines stay
mandatory. Managed growth and resident execution already used this CPU policy.
No CPU threshold was increased, and no new concurrency limit was added.

The regression replays the measured 35/36/37/38 initial owners with cached
CPU 98/97/84/98 and a mixture of completed/pending creates. Final distribution is
64/64/64/64. An assembled service test holds one create inside the existing
single startup permit at 100% CPU: the second create queues, neither is rerouted
or rejected solely for CPU, and both complete when the permit is released.
Memory, unknown evidence, shape, disk, tmpfs, drain, stale owner and admission
deadline tests remain covered. The first local gate passed 205 tests. Production
qualification of this change remains separate from that regression evidence.

Raw placements, resource observations and sampled create traces are in
`rc19-pressure-placement-evidence.json`. The final 73-versus-55 difference is
harmful load concentration; this report does not claim it is the sole cause of
the pressure run's late managed-start failure.
