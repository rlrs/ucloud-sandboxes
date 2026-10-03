# Why pause reclaim freed ~13 MB per sandbox (2026-10-03)

**Question.** In [memory-pressure-2026-10-03](../memory-pressure-2026-10-03/README.md)
the C1.1 pause tier ran 586 reclaims. Together they freed 7.7 GB, against
about 1.6 GB resident per paused sandbox, and 225 waits escalated to
hibernation. Why?

**Answer: zswap.** The guest's pages are charged to the right cgroup, and
`memory.reclaim` moves them. But zswap takes incompressible pages into its
pool at almost 1:1, and that pool is charged to the same cgroup. So
`memory.current` barely drops.
- The reclaim loop then correctly sees no progress (`not_shrinking`).
- The stall rule marks the wait stalled for good, and it escalates to
  hibernation.
- With zswap off for the cgroup, even production's unchanged loop frees
  everything.

## Probe

[reclaim_probe.py](reclaim_probe.py) ran on one disposable, unregistered CCX63:
kernel 7.0.0-30, the 0.8.4 bundle and the pause-tier config. zswap was on
(zstd, `max_pool_percent` 20, `accept_threshold_percent` 90) over a 128 GiB
NVMe swap file.

Each of four managed sandboxes:
- holds a 1.5 GiB random heap;
- works once, then pauses through a relay-shaped park;
- has one reclaim variant written straight to its own cgroup,
  `/sys/fs/cgroup/ucloud-sandboxes/<sha256(id:generation)>`;
- is then woken, and works once more with its heap verified. The same
  process came back intact in every case.

Raw results: [raw/reclaim.json](raw/reclaim.json).

| variant | writes | time | net freed (`memory.current`) | moved to swap/zswap | zswap pool after | wake | work after wake (first work 2.1 s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A: production loop (16 MiB windows, stop below 1 MiB), zswap on | 4 | 0.9 s | **31 MB** | 63 MB | 35 MB | 0.14 s | 2.6 s |
| B: one write for all of `memory.current`, zswap on | 1 | 12.4 s | **32 MB** | 1,568 MB | **1,540 MB** | 0.27 s | 3.1 s |
| C: one write, `memory.zswap.max=0` | 1 | 8.4 s | **1,573 MB** (188 MB/s) | 1,569 MB | 0 | 1.6 s | 10.1 s |
| E: production loop, `memory.zswap.max=0` | 99 | 15.1 s | **1,574 MB** (105 MB/s) | 1,569 MB | 0 | 1.6 s | 9.8 s |

- **B is the mechanism.** 1,563 MB of random pages were compressed into a
  1,540 MB pool charged to the cgroup. Pages "left" the cgroup's memory
  without leaving its charge. A (production) is B cut short by the stop rule
  after four windows.
- **C and E show the rest of the path works.** The cgroup charge is right,
  writeback to NVMe swap runs at about 190 MB/s for one reclaim, and the stop
  rule does not trigger when real progress happens. 16 MiB windows halve the
  rate against one write (105 against 188 MB/s).
- **The way back costs refault.** Thaw prefetch is capped at 1 GiB or 2 s;
  the wake took 1.6 s. The rest of the heap came back by faults, so the first
  work took 10 s. That is the worst case: this workload re-hashes its entire
  1.5 GiB heap every cycle. A real agent touches a working set.
- **The random heap is the worst case for zswap.** Real agent memory (Python
  heaps, compilers, page tables) compresses, commonly 2–4×. In production,
  zswap is right for some sandboxes and wrong for others.

## What DSec does, and what C1.1 must match

The plan's target (§1 move 2 and §4 "Memory") is DSec's container pause:
- freeze, then `memory.reclaim` to zswap or NVMe swap, then prefetch on thaw;
- pause costs track the pages actually evicted, with no snapshot format and
  no disk claim;
- hibernation only for drain, offload or long predicted waits;
- the node as the final admission authority.

Measured against that:

| | DSec target | C1.1 as built | Effect in the pressure run |
| --- | --- | --- | --- |
| Eviction | reclaim to zswap or swap until the target is met | 16 MiB windows; zswap for every page, compressible or not | about 13 MB per reclaim; the pool absorbs incompressible pages |
| No progress | rare; costs track evicted pages | permanent stall, then hibernate | 234 stalls and 225 hibernations |
| Choosing whom to evict | the waits that will stay idle longest | most time already paused, so the waits closest to waking | 131 thaws cancelled reclaims |
| Wake against new work | the running rollout keeps its claim; the node decides | wakes pay full headroom (swapped bytes count as debt) and compete with creates | median wake 9–16 s while new rollouts start |
| Thaw | `MADV_WILLNEED`-style prefetch | `MADV_POPULATE_READ` prefetch, 1 GiB or 2 s | works (1.6 s wake); the rest is demand faults |

## Fix design (C1.1, second pass)

1. **Bound zswap per paused cgroup.**
   - When zswap is on, a paused sandbox may hold at most a fixed share (25%)
     of its memory bound in zswap: `memory.zswap.max`, set once at pause. The
     kernel sends the rest to NVMe swap.
   - This is a fixed, kernel-native bound, whatever the memory compresses to.
     A per-reclaim "measure the ratio, then switch zswap off" rule was
     rejected as a runtime heuristic.
   - zswap stays off by default (`direct_pause_tier_zswap`). The fleet's real
     compression ratio, read from `memory.stat`, decides whether to turn it
     on.
2. **Reclaim to the target in larger windows** (128 MiB), still cancellable
   between windows. Record each reclaim's stop reason; it is dropped today
   (`node_runtime.py:457-461`).
3. **No permanent stall.** A reclaim that made no progress is retried after a
   backoff. A wait escalates to hibernation only when swap room or reclaim
   cannot cover the deficit, which is DSec's "hibernate for offload".
4. **Evict the waits that will stay idle longest.**
   - Rank by expected remaining wait (the relay's `resource_phase` hint, the
     Aries rule's input).
   - Without a hint, prefer the most recently paused over the longest paused.
5. **Admission: running rollouts first.**
   - A wake for an answered model wait outranks a create.
   - Wake debt counts only the prefetch budget, not every swapped byte;
     faults bring the rest, as DSec's thaw does.
   - A create may evict a wait only by pausing it, or by hibernating it when
     its expected remaining wait is long.

   This is a node admission change (`direct_service.py`, `warm_park.py`) and
   also changes today's non-pause path, so it ships separately.
6. **Make model waits node-local** (from
   [rl-scale-relay-2026-10-03](../rl-scale-relay-2026-10-03/README.md)). The
   relay tells the node directly, and pause or reclaim needs no gateway
   round trip.

Items 1–4 shipped in `668f547`. Item 5 (admission) shipped in `b39f00b` and
was validated in [admission-priority-2026-10-03](../admission-priority-2026-10-03/README.md):
modest on today's path, neutral on the pause tier. Item 6 (node-local waits)
remains.

## Pass 2: the second pass under the same pressure (2026-10-03)

- **Setup.** Two disposable, unregistered CCX63, on bundle `94d5a908`
  (0.8.4 plus `668f547`), with the pause tier, zswap on and 128 GiB of swap.
  The same 140-rollout relay harness ran on each, now retrying deferred wakes
  as the relay does:
  - random heaps on one node;
  - about 4:1 compressible heaps (`--compressible`) on the other.
- **Raw evidence:** [raw/pass2-random.json.gz](raw/pass2-random.json.gz) and
  [raw/pass2-compress.json.gz](raw/pass2-compress.json.gz).

| 140 rollouts, one CCX63 | pass 1 (random) | **pass 2, random** | **pass 2, compressible** | today's policy |
| --- | ---: | ---: | ---: | ---: |
| reclaims; bytes freed | 586; 7.7 GB (13 MB each) | **80; 26.2 GB (327 MB each)** | **42; 41.0 GB (977 MB each)** | — |
| stalls; escalations to hibernate | 234; 225 | **15; 2** | **5; 0** | — (181 checkpoints) |
| how reclaims stopped (target, not shrinking, partial, errors) | not recorded | 3, 53, 3, 0 | 12, 25, 0, 0 | — |
| waits that ended hibernated or parked | 188 of 879 | **2 of 963** | **0 of 936** | 146 of 877 |
| wake p50 / p95 | 43 ms / 1.3 s, and 9–13 s once hibernated | **37 ms / 0.72 s** | **37 ms / 0.72 s** | 59 ms / 1.9 s, and 14–16 s once hibernated |
| cycles completed | 974 | 1,075 | 1,048 | 965 |
| swap used, most; zswap pool, most | 1.5 GB; 0.6 GB | 4.8 GB; 2.1 GB | 5.7 GB; 1.3 GB | — |
| memory PSI full, highest avg10 | 0.65 | 11.8 | 1.1 | 0 |

**Reading it:**
- **The middle tier works.** Paused rollouts now give memory back through
  swap rather than being hibernated. Escalations fell from 225 to 2 and 0,
  and almost every wait returns as a resume, not a restore.
- **Compressible memory gains most.** Each reclaim freed about 1 GB. Random
  memory frees less per reclaim, because up to a quarter of the bound stays
  in zswap. It thrashes more too (PSI full 11.8): incompressible pages cost a
  compression attempt, then a write.
- **The tail was mostly the harness.** In this harness, finished rollouts
  were never deleted.
  - 11 and 16 of about 950 wakes per node took over 5 s; a few took 3–5
    minutes after up to 7 retries.
  - The 2 "deaths" per node are late-created sandboxes whose wakes were still
    retrying when the run was interrupted.
  - Both come from late wakes and creates waiting for headroom that finished
    rollouts held forever. With rollouts that end
    ([admission-priority-2026-10-03](../admission-priority-2026-10-03/README.md)),
    this tier's wakes have a p95 of 0.13 s and a maximum of 3.1 s.
- **Density:** 128 created against 124–130, which is the node's capacity at
  2 GiB bounds.

**Validation (as planned before pass 2):** re-run `benchmark_sandbox_density.py --mode relay` (140
rollouts on one CCX63), with wakes retried as the relay does.
- **Reproducing these numbers:** pass `--compressible` heaps once the harness
  has them.
- **Gates:** no agent lost; escalations a small fraction of waits; reclaim
  freeing most of each target; wake p95 within C1.1's budget plus refault.

## Resources

- Probe: one CCX63 for about 25 minutes, deleted afterwards.
- Pass 2: two CCX63 for about 30 minutes, deleted afterwards. Neither was
  ever registered.
- It never registered with the gateway (`gateway_port: 1`).
