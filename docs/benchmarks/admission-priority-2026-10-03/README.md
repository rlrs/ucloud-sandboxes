# Admission puts running rollouts first: validation (2026-10-03)

**Question.** Item 5 of
[pause-reclaim-2026-10-03](../pause-reclaim-2026-10-03/README.md) (`b39f00b`)
changes admission in two ways:
- a launch must leave headroom for every queued continuation and restore, not
  only the head one;
- a continuation whose pages a pause reclaimed owes its thaw prefetch, not its
  whole swapped footprint.

Does that shorten wakes under memory pressure, on today's path and on the
pause tier?

**Answer: a modest gain on today's path, and none measurable on the pause
tier.**
- **Today's path.** Hibernated wakes went from 12.3 s to 9.9 s at the median.
  All wakes went from 14.2 s to 10.6 s at p95, and the maximum from 57 s to
  32 s. There were 137 checkpoints, against 160.
- **Pause tier.** Every wait already comes back as a resume (p95 0.13 s
  against 0.18 s, single runs), so there is little left for admission to fix.
- **A correction.** The minutes-long wakes that the earlier pause-tier run
  blamed on admission came mostly from the harness. Finished rollouts were
  never deleted, so late creates and wakes waited for headroom that never
  freed. With rollouts that end, they are gone on both versions.

The large effect remains the pause tier itself (items 1–4). At the same load,
a wake takes 0.13 s at p95 against 10–14 s on today's path, and nothing
hibernates.

## Setup

- **Workers.** Four disposable CCX63 runs, two at a time, from snapshot
  `438866767`. All ran the live config with `gateway_port: 1`, so none ever
  registered.
  - "Pause" runs add `swap_gb: 128`, the pause tier and zswap (zstd).
  - "Today" runs use the production config unchanged.
- **Bundles:**
  - 0.8.4 (`4c098477`);
  - items 1–4, `0.8.5.dev0+pause2` (`94d5a908`);
  - items 1–5, `0.8.5.dev0+pause3` (`3f6f75fc`).
- **Harness.** `scripts/benchmark_sandbox_density.py --mode relay` (`d1991aa`).
  - 140 managed agents on one node, each holding a 1.5 GiB random heap.
  - Each runs 8 cycles: work, a relay-shaped park, a 20 ± 5 s model wait, and
    a wake. Wakes are retried as the relay does.
  - A rollout then ends and its sandbox is deleted, so queued creates take its
    place.
  - Every run ended on its own: 140 of 140 rollouts finished, with no lost
    agent and no manual step.
- **Raw evidence:** [raw/](raw/) (`base-*` and `item5-*`). The counters below
  are deltas within each run.

## Results

| 140 rollouts, one CCX63 | 0.8.4 today | today + item 5 | pause tier, items 1–4 | pause tier, items 1–5 |
| --- | ---: | ---: | ---: | ---: |
| finished; makespan | 140; 471 s | 140; 458 s | 140; 449 s | 140; 448 s |
| hibernations (checkpoints) | 160 | 137 | 0 | 0 |
| pause reclaims; freed; stalls; escalations | — | — | 30; 7.5 GB; 3; 0 | 31; 8.2 GB; 3; 0 |
| waits ending hibernated or parked | 157 of 1,120 | 136 of 1,120 | 0 | 0 |
| wake p50 / p95 / p99 / max | 0.02 / 14.2 / 24.1 / 57 s | 0.03 / 10.6 / 26.6 / 32 s | 0.03 / 0.13 / 0.94 / 3.1 s | 0.03 / 0.18 / 1.2 / 11.1 s |
| hibernated wake p50 / p95 | 12.3 / 26.5 s | 9.9 / 29.4 s | — | — |
| wakes over 5 s | 112 | 101 | 0 | 3 |
| creates that waited for admission; create p95 | 19; 134 s | 20; 137 s | 17; 131 s | 17; 126 s |
| lowest MemAvailable; highest PSI full | 10 GB; 0 | 8 GB; 0 | 3 GB; 3.2 | 2 GB; 5.6 |

## Reading it

- **Item 5 helps today's path a little.** Fewer launches spend the headroom
  that queued wakes need, so slightly fewer waits are hibernated (137 against
  160). Their wakes are somewhat shorter at the median and the maximum, but
  not at p95 or p99. These are single runs, and the differences are within
  what a repeat could move.
- **Item 5 changes nothing measurable on the pause tier.** Wakes are resumes,
  and their prefetch-sized debt fits; the 11 s maximum is one wake in 1,120.
- **The slow wake on today's path is the restore itself:**
  - a full checkpoint restore of a 1.5 GiB guest, 10–12 s at the median;
  - plus the wait for its memory bound.

  Admission can only reorder who waits. The pause tier removes the restore.
- **Creates queue the same way in all four runs** (17–20 waited, p95 about
  130 s). That is the node's real capacity at 2 GiB bounds; admission was not
  meant to change it.

## Decision

- **Keep item 5.** It is DSec's rule (running work first), cheap, and never
  worse in these runs.
- **Do not count on it for latency.** The lever is turning on the pause tier,
  with items 1–4, which needs swap on the workers.
- **Make the pause tier the production default next.** C1.1's gates, measured
  here at 140 rollouts on one CCX63:
  - pause and thaw within budget: wake p95 0.13 s;
  - no SIGBUS and no lost agent;
  - physical I/O well below hibernation's.

  The rollout needs a worker snapshot with swap and a canary.

## Resources

- Four CCX63 runs of about 25 minutes each, two at a time.
- Before them, a truncated attempt on the item 5 workers whose evidence is not
  used. It was interrupted by hand under the old harness.
- All workers were deleted, and their known_hosts entries cleared. The gateway
  listed 0 nodes throughout.
