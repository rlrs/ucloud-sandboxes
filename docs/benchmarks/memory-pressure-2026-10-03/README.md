# Model waits under memory pressure: today's policy against the pause tier (2026-10-03)

**Question.** Production agents wait on the model through the relay. What
happens when a node has more rollouts than memory, under each policy?
- **Today's policy:** keep waits resident, then hibernate under pressure.
- **The pause tier (C1.1):** pause, `memory.reclaim` to zswap/swap, and the
  Aries rule.

**Answer.**
- **Nothing crashed.** No agent died and there was no SIGBUS. Admission keeps
  every sandbox's memory bound inside RAM, which closes the 2026-09-23 tmpfs
  exhaustion path.
- **The cost is latency.** Both policies admit new work by evicting waiting
  rollouts, which then wait 9–16 s at the median, and up to 52 s, to be
  restored.
- **The pause tier adds no density as built.** Its reclaim moves almost
  nothing to swap: 7.7 GB over 586 reclaims, and at most 1.5 GB of swap used.
  It escalates to hibernation the way today's policy does.

## Setup

- **Workers.** Two disposable CCX63 (48 vCPU, 184 GB) from snapshot
  `438866767`, on the unchanged 0.8.4 bundle and the live production config
  with two changes:
  - **`gateway_port: 1`.** The workers never registered with the gateway, so
    production could not place work on them. The gateway listed 0 nodes
    throughout.
  - **The pause worker only:** `swap_gb: 128`, `direct_pause_tier: true` and
    `direct_pause_tier_zswap: true` (zstd). Its application-memory tmpfs is
    then swappable, at 303 GiB against 175 GiB with `noswap`.
- **Driver.** [`scripts/benchmark_sandbox_density.py --mode relay`](../../../scripts/benchmark_sandbox_density.py),
  run on each node against its own agent API. Every sandbox is a **managed
  process**, like production's relay agents, so idle parking never applies.
  It holds a 1.5 GiB heap. Each cycle:
  1. **Work:** verify the heap's sha256, dirty 384 MiB, write 64 files,
     commit SQLite.
  2. **A relay-shaped park:** `relay_request_id` and `generation`, exactly as
     the gateway forwards a model wait.
  3. **A model wait:** 20 ± 5 s.
  4. **A wake.**
- **Load.** 140 rollouts per node, 8 cycles, created 16 at a time. Each
  sandbox starts its loop as soon as it is created, so later creates queue for
  admission while earlier rollouts already wait on the model. Bounds are
  2 GiB and 1 vCPU each.
- **Harness fixes on the way:**
  - Parkable sandboxes that are not managed are hibernated after 1 s idle
    (`direct_idle_park_seconds`), which made the first attempt measure idle
    churn.
  - The first probe needed more than 60 s to fill a heap.
  - Admission returns retryable 503s that the harness must retry.
  - Running all creates before any wait never produced pressure. Admission
    capped each node at about 86 sandboxes.

## Results

| | today's policy | pause tier |
| --- | ---: | ---: |
| rollouts created (of 140) | 130 | 124 |
| creates that waited for admission | 17 (p95 16 min) | 14 (p95 18 min) |
| model waits | 877 | 879 |
| node's answer to the park | 877 deferred (stay resident) | 879 accepted as pause |
| waits that ended hibernated or parked | 146 (17%) | 188 (21%) |
| full checkpoints / pause escalations to hibernate | 181 / — | — / 225 |
| pauses / thaws / reclaims | — | 945 / 895 / 586 |
| bytes reclaimed by pause | — | 7.7 GB (13 MB per reclaim) |
| swap used at most; zswap | — | 1.5 GB; 0.6 GB |
| lowest MemAvailable | 11.2 GB | 4.9 GB |
| memory PSI full, highest avg10 | 0.00 | 0.65 |
| agents lost | 0 | 0 |

Raw evidence: [raw/today.json.gz](raw/today.json.gz) and
[raw/pause.json.gz](raw/pause.json.gz).

**Wake latency** (the answer arrives, until the node lets the sandbox run), by
what the wait turned into. The values are p50 / p95 / max in seconds:

| state before wake | today's policy | pause tier |
| --- | --- | --- |
| resident or paused | 0.059 / 1.9 / 31 (n 731) | 0.043 / 1.3 / 37 (n 691) |
| hibernated | 16.0 / 35.7 / 44.7 (n 91) | 9.4 / 28.6 / 41.2 (n 122) |
| parked | 14.0 / 40.9 / 52.0 (n 55) | 12.9 / 39.8 / 44.4 (n 66) |

**Work after the wake** (heap verify, dirty 384 MiB, files, SQLite): 2.2–2.4 s
at p50 whatever the wait became, against 2.1 s on the first cycle. Refault and
restore cost hardly shows; the wait for headroom dominates.

**The 53 "deaths" were refused wakes, not dead agents.**
- 33 on today's policy and 20 on the pause tier were wakes the node answered
  with a retryable 503 (`node_startup_busy` or `node_restore_busy`), because
  there was no headroom to restore.
- Their processes were intact and their sandboxes parked.
- The harness does not retry wakes. Production's relay does, so in production
  these are longer waits, not lost rollouts.
- At the end, no sandbox was waiting and the remaining creates sat in
  admission. The run was stopped with SIGINT, after 16 finished sandboxes per
  node were deleted to release the held requests.

## Reading it

1. **Pressure is a scheduling problem now, not a crash.** Admission holds back
   headroom for every admitted sandbox to reach its bound
   (`admitted_demand_bytes`), so the node admits about RAM ÷ bound sandboxes:
   about 86 at 2 GiB.
   - Above that, new creates are "incoming demand".
   - The node makes room by hibernating rollouts that are waiting on the
     model.
2. **Those rollouts then wait to come back.** A restore needs the same
   headroom the newly admitted sandboxes just took. So a model answer can sit
   9–16 s at the median, and up to 52 s, before the agent runs again, while
   new rollouts start.
   - For RL this is the wrong priority. Finishing a rollout that already holds
     state should beat starting a new one, and DSec gives running work
     priority.
   - The wake should outrank creates in admission. The create path should not
     evict a wait whose answer is due in seconds.
3. **The pause tier does not deliver its middle tier:**
   - Reclaim freed 13 MB per paused sandbox, against about 1.6 GB resident.
   - Swap stayed nearly empty, and 225 waits escalated to hibernation.
   - Two likely causes, to verify next:
     1. Admission counts bounds, not resident bytes, so reclaim cannot create
        admission headroom. Only escalation can.
     2. The guest's memory lives on the shared application-memory tmpfs. If
        those pages are not charged to the sandbox's cgroup, `memory.reclaim`
        on it cannot evict them.
   - Without a working reclaim, the pause tier is today's policy plus pause
     and thaw overhead.
4. **Today's 1 s idle parking is invisible to relay agents,** because managed
   processes are skipped. It applies to parkable sandboxes that are driven
   only by exec.

## Next

1. **Prioritize wakes over creates in admission.** A restore for an answered
   model wait goes first. A create may evict a resident wait only when its
   expected remaining wait is long, which is the Aries rule with relay
   `resource_phase` hints.
2. **Make pause reclaim work:**
   - Find where guest tmpfs pages are charged.
   - Reclaim against that cgroup, or move the memory file's charge to the
     sandbox.
   - Let admission count reclaimed and paused bytes as swap-backed, so a
     paused rollout frees admission headroom without a checkpoint.
3. **Re-run this harness** with wakes retried as the relay does, and add a
   resident-wait arm with no create pressure as the floor.
4. **Then make model waits node-local,** as
   [rl-scale-relay-2026-10-03](../rl-scale-relay-2026-10-03/README.md)
   proposes.

## Resources

- Two CCX63 for about 50 minutes.
- Deleted afterwards, with their known_hosts entries; nothing was ever
  registered.
- `/work/ucloud-sandboxes/pressure-20261003` on the gateway holds the derived
  configs and helper scripts.
