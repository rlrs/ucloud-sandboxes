# Node create pipeline (C5.2), 2026-10-01

Before and after node-side create phases for plan item C5.2
([`rl-scale-architecture-plan.md`](../../rl-scale-architecture-plan.md), W5).

**Change measured.**

- Registry commits per create drop from four (`plan`, `commit_quota`,
  `commit_rootfs`, `commit_owned`) to three. The quota rides on
  `commit_rootfs`.
- New leases take a pre-created netns+veth pair from a pool of 32.
- The per-create host-rule check is unchanged on purpose.

**Host.** Development box: 30 CPUs, Linux 5.15, ext4 on a shared disk, Python
3.10. Other agents ran test suites at the same time, so the ext4 rows are
noisy. Each run ran in a disposable user, network and mount namespace
(`unshare --user --map-root-user --net --mount`) with a private `/run/netns`.
The host network was not touched.

**Real and fake parts.**

- Real: `DirectSandboxProvisioner`, `DirectSandboxRegistry` (SQLite,
  `synchronous=FULL`), and `DirectNetworkManager` with real `ip`, netns, veth,
  bind mounts, `iptables-save` and `sysctl` inside the namespace.
- Fake: the storage service, overlay mounts and runsc. These are the
  provisioner test fakes of the measured tree.

**Configurations.**

- `baseline`: the tree before C5.2.
- `3 commits`: C5.2 with `pool_size=0`.
- `3 commits + pool`: C5.2 with `pool_size=32`.

**Scenarios.**

- Sequential: 100 creates.
- Burst: 32 concurrent creates, starting with a full pool.
- Sustained: 128 creates at 32-way concurrency.

State was on ext4 (one fsync is about 10 ms here) or on tmpfs, where fsync is
free and only CPU, subprocess and kernel work remain. Values are the medians,
over three interleaved runs, of each run's p50 / p99 in ms.
[`summary.json`](summary.json) has every phase.

## Results

### tmpfs

| Scenario | baseline | 3 commits | 3 commits + pool |
| --- | ---: | ---: | ---: |
| Sequential: total | 13 / 14 | 12 / 14 | **6 / 13** |
| Sequential: `network_ensure` | 10 / 11 | 9 / 10 | **3 / 11** |
| Burst of 32: total | 309 / 313 | 274 / 276 | **191 / 198** |
| Burst of 32: creates/s | 97.6 | 108.9 | **146.5** |
| Burst of 32: `registry_commit` + `storage_prepare` p50 | 190 + 91 | 241 + 0 | 174 + 0 |
| Burst of 32: `network_ensure` | 36 / 48 | 28 / 56 | **11 / 47** |
| Sustained 128 at 32-way: total | 290 / 305 | 249 / 276 | 254 / 270 |
| Sustained 128 at 32-way: creates/s | 109.1 | 125.8 | 135.0 |

### ext4

| Scenario | baseline | 3 commits | 3 commits + pool |
| --- | ---: | ---: | ---: |
| Sequential: total | 105 / 196 | 90 / 126 | 88 / 127 |
| Burst of 32: total | 989 / 1370 | 1200 / 1341 | 677 / 1030 |
| Sustained 128 at 32-way: total | 1175 / 1357 | 1125 / 1370 | 1026 / 1301 |

In the baseline, `storage_prepare` contains the quota commit. The
registry's activity revision confirms 4.0 commits per create before and 3.0
after, in every scenario.

## Findings

- **The pool takes `ip` off the create path.** A pool hit runs no `ip`
  process. It needs one durable lease write, one `mount(MS_BIND)`, one
  `umount2` and one `SIOCGIFINDEX`. Sequential `network_ensure` falls from
  10 ms to 3 ms. Most of the 3 ms is the host-rule check, whose two
  subprocesses remain; phases are truncated to whole milliseconds, so it
  reports 2 ms.
- **The host-rule check costs 2.6–3.0 ms.** One `iptables-save` takes
  1.3–1.5 ms and one `sysctl` about 1.0 ms, on this near-empty ruleset.
  Production rulesets are larger, and `iptables-save` grows with them.
  Creates now report the check as `network_host_rules_ms`. Under concurrency,
  queued creates share one check. With the pool, the burst p50 is 0 ms and
  the create that runs it pays the p99.
- **At 32-way concurrency the registry writer dominates even without fsync.**
  On tmpfs, 3 × 32 write transactions take 170–240 ms of each create, although
  one transaction alone takes under 1 ms. The likely cause, not yet profiled:
  the `_writer_turn` holder releases the GIL inside every SQLite call and,
  with 31 runnable threads, can wait up to the 5 ms switch interval to get it
  back. The next lever is a single
  writer with group commit or an in-memory index (plan C5.2, third bullet),
  not fewer fsyncs. The plan gate is p50 ≤ 150 ms and p99 ≤ 400 ms at 32
  concurrent creates. With fakes on tmpfs the burst gives 191 / 198 ms: p99
  passes and p50 does not.
- **Sustained load is not helped by the pool, by design.** The pool covers
  one burst. The refill defers to in-flight creates for up to 1 s per pair,
  so under a constant stream it adds about one pair a second. Pool misses
  build their pair synchronously, as before.
- **On ext4, fsync throughput bounds everything.** Each create also makes the
  durable lease write: the file and its directory are fsynced under the state
  lock. That write is unchanged, because it is the slot's durability.
- **No `SCHED_IDLE` for the refill thread.** A first matrix ran the refill at
  `SCHED_IDLE`. ext4 sustained `network_ensure` p50 was 1.6–1.7 s in two of
  three runs, against 0.5–0.9 s without it. The thread shares the GIL and
  the state lock with creates, so starving it while it holds either stalls
  them. The evidence is suggestive on a noisy disk, but the mechanism is
  enough to rule it out. Low priority now comes from deferring to creates.
  The cgroup `bg` class (C1.5) fits separate processes, not Python threads.

## Reproduce

```sh
# Export the pre-change tree to compare against, then for each code tree,
# medium and pool size:
unshare --user --map-root-user --net --mount --fork \
  .venv/bin/python docs/benchmarks/create-pipeline-2026-10-01/bench_create_pipeline.py \
  --code "$TREE" --state "$STATE_DIR" --out result.json --pool 32 [--tmpfs]
```

`--pool` is ignored for a tree without the pool.
