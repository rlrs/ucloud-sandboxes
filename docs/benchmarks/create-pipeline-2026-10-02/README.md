# Node create pipeline: registry index and durability audit (C5.2), 2026-10-02

Before and after for the rest of plan item C5.2
([`rl-scale-architecture-plan.md`](../../rl-scale-architecture-plan.md), W5),
following [the 2026-10-01 run](../create-pipeline-2026-10-01/README.md).

**Change measured.**

- **Owner index.** The node agent's `DirectSandboxRegistry(owner=True)`
  takes an exclusive `flock` on `direct-registry.sqlite.owner` for its
  lifetime, so a second live owner in any process is refused. The kernel
  drops the lock when the process dies. The owner serves `get`, `snapshot`,
  `activity_revision` and `references_image` from memory, with no SQLite
  statement and no syscall. `disk_claims_mb` reads SQLite once per activity
  revision and serves that result until the revision moves.
  - Writes run on one owner connection under the writer turn, and reach the
    index only after `COMMIT` returns.
  - Every write's first statement carries `pragma_data_version`. SQLite
    changes it on every commit by another connection, never on the
    connection's own (another connection's WAL truncation also changes it,
    which costs only a rebuild). A foreign commit therefore rebuilds the
    index from that transaction's snapshot before the write runs. Reads
    recheck at most once a second.
  - A failed `COMMIT`, or a `ROLLBACK` that fails and leaves the
    transaction open, drops both the index and the owner connection.
  - Point reads no longer need the rc43 single-statement read or the
    decoded-record cache, so both are gone.
- **`commit_owned` is not fsynced** (`synchronous=NORMAL` for that one
  transaction). See the audit below.
- **Pool-only network writes skip the directory fsync.** Lease writes still
  sync both the file and the directory.
- **Memoized on each immutable record:** the spec fingerprint and
  `to_direct_sandbox()`. The heartbeat derived both several times per
  registration.

**Host, method and guard.** As on 2026-10-01: a shared 30-CPU development box
on Linux 5.15. Other agents' test suites kept the load average at 12–16, so
ext4 rows are noisy. Each run happens in a disposable
`unshare --user --map-root-user --net --mount` namespace with a private
`/run/netns`. The script asserts it is not host root, so it never touches
the host network. [`bench_create_pipeline.py`](bench_create_pipeline.py)
extends the earlier script:

- The registry is the owner instance when the tree has one.
- After the create scenarios, it tops the node up to 500 registrations and
  times `DirectNodeRuntime.heartbeat_snapshot`, with fakes for storage,
  overlays and runsc as before. It times the heartbeat both with no write
  between builds and after one registry write.

**Configurations.**

| Name | Tree |
| --- | --- |
| `before` | The tree before this change, which includes C5.2 part 1 (3 commits per create, 32-slot pool). |
| `after` | This change. |
| `after_full_owned` | This change, with `commit_owned` fsynced. Isolates the `NORMAL` change. |

Values are the medians, over three interleaved runs, of each run's p50 / p99
in ms. [`summary.json`](summary.json) has every phase.

## Results

### Creates (tmpfs: fsync is free)

| Scenario | before | after | after_full_owned |
| --- | ---: | ---: | ---: |
| Sequential (1 at a time): total | 6.0 / 13.3 | 10.5 / 13.6 ¹ | 10.6 / 12.6 ¹ |
| Burst of 32: total | 178.6 / 183.6 | **122.6 / 124.0** | 125.2 / 126.9 |
| Burst of 32: `registry_commit` | 165 / 172 | **102 / 108** | 106 / 113 |
| Burst of 32: creates/s | 155.4 | **218.8** | 213.1 |
| Sustained 128 at 32-way: total | 254.4 / 265.5 | 220.9 / 257.5 | 222.2 / 254.5 |
| Sustained: creates/s | 135.5 | 155.6 | 154.5 |

¹ Sequential creates are bimodal. A create that finds a ready pooled pair
spends 3 ms in `network_ensure`, and one that misses spends 9–10 ms. In two
of three matrix runs, both `after` trees missed more often, under the
matrix's host load. An 8-run sequential-only A/B
([`sequential-ab.json`](sequential-ab.json)) gives p50 5.9–7.4 ms before and
5.5–5.9 ms after, with every run hitting the pool. The registry is under
1 ms per sequential create in every configuration.

### Creates (ext4, shared disk)

| Scenario | before | after | after_full_owned |
| --- | ---: | ---: | ---: |
| Sequential: total | 56.9 / 204.8 | 51.5 / 60.1 | 51.0 / 72.8 |
| Burst of 32: total | 518.7 / 607.2 | 386.1 / 567.6 | 379.9 / 617.0 |
| Burst of 32: `registry_commit` | 390 / 405 | 113 / 168 | 85 / 138 |
| Burst of 32: `network_ensure` | 134 / 265 | 274 / 382 | 265 / 461 |
| Sustained 128 at 32-way: total | 659.7 / 800.5 | 553.6 / 622.3 | 594.2 / 655.6 |
| Sustained: creates/s | 47.1 | 57.0 | 51.7 |

Runs varied up to 3x on this disk. Treat the ext4 deltas between `after` and
`after_full_owned` as noise.

### Heartbeat inventory and registry reads at 500 registrations

| Measurement | before (tmpfs / ext4) | after (tmpfs / ext4) |
| --- | ---: | ---: |
| `heartbeat_snapshot`, no write between | 107.1 / 98.4 | **5.7 / 5.4** |
| `heartbeat_snapshot` after one registry write | 99.4 / 98.5 | **6.4 / 6.5** |
| `registry.snapshot()` | 0.84 | 0.001 |
| `registry.disk_claims_mb()` | 0.41 | 0.002 |
| `registry.get()` | 21 µs | ≤ 1 µs |
| `registry.activity_revision()` | 18 µs | ≤ 1 µs |

The timer rounds to 1 µs. All values are p50 in ms unless marked otherwise.

## Durability audit of the create path

`synchronous=NORMAL` in WAL mode keeps every commit atomic, and process death
loses nothing. Only an OS crash or power loss can lose commits, and only those
after the last FULL commit or checkpoint. Because the WAL is ordered with
cumulative checksums, a later FULL commit syncs every earlier frame. Mixing
NORMAL and FULL transactions on one connection is therefore safe. An OS crash
also kills every sentry, mount and network namespace, so what matters is the
state that recovery reads after a reboot.

| Store | Write | Class | Level | Crash scenario if it were not synced |
| --- | --- | --- | --- | --- |
| direct registry | `plan` | authority: precedes the storage, memory and network claims | FULL | The volume, project ID and memory allocation would exist with no owner record, leaking capacity, and a replay could plan over them. |
| | `commit_rootfs` (with quota) | authority: precedes runsc create | FULL | Registry `planned` with a journaled runtime. Recovery from `planned` runs `discard_unregistered` and rebuilds the writable layer under a live journal, losing user data. |
| | `commit_owned` | **derivable** | **NORMAL** | Registry `rootfs_ready` with the Warden journal, which is fsynced after runsc start and before `commit_owned`. Recovery now advances a journaled `rootfs_ready` exactly as an owned registration, to the same revision, with no guest-file replay. Any later FULL write that depends on `owned` (delete, move, claims, growth) syncs it first. |
| | `begin_delete`, `commit_deleted`, tombstones | authority: fences | FULL | A delayed create of a deleted generation could succeed. |
| | migration phases | authority: digest-fenced across nodes | FULL | Two nodes could own one incarnation. |
| | `registration_disk`, `workspace_capacity`, `reflink_overlaps`, claim updates | authority: hard-disk admission | FULL | The node would over-admit disk after a reboot. |
| | `managed_growth` + `relay_wake_fences` (one commit) | authority: a committed wake supersedes an older park | FULL | An old park could win after a reboot. |
| | `drain_json`, runtime compatibility | authority, and rare | FULL | Admission could reopen under an autoscaler drain. |
| memory-backing journal | claim (`preparing`), `ready`, retained checkpoints | authority: project IDs and capacity before quota provisioning; already one FULL commit per batch | FULL | It is a separate file, so a registry fsync does not sync it. A lost `ready` under a committed `rootfs_ready` would leave the allocation in `preparing`. |
| storage-native journal (root daemon) | volumes, operations, counters, retired devices | authority: precedes device and project changes; idempotency; batched | FULL | A lost completion would leave the volume pending under a committed registration. |
| hibernation journal (fsynced files, not SQLite) | lifecycle records | authority; the reason `owned` is derivable | fsync | – |
| `network-slots.json` (not SQLite) | leases, relay policies | authority: slot → guest IP across parks | file + dir fsync | A reverted lease could hand a parked sandbox's IP to another. |
| | pool-only writes | **derivable**: pooled pairs die with the kernel and start rechecks every pooled slot | **file fsync only** | After a crash, the name holds this complete write or the last synced one, with the same leases and policies. |
| metrics journal | telemetry events | advisory | NORMAL, separate file (unchanged) | – |

No registry transaction other than `commit_owned` is provably safe at NORMAL.
No other table is advisory enough to move into a NORMAL database without
splitting an atomic commit. `managed_growth` comes closest, but its
activation commits a wake fence in the same transaction.

## Findings

- **The 32-way gate passes on tmpfs with fakes.** p50 is 122.6 ms against a
  target of 150, and p99 is 124.0 ms against 400. The 2026-10-01 tree gave
  191 / 198 in that run and 178.6 / 183.6 here.
- **The registry writer still dominates a burst.** 3 × 32 transactions spend
  about 100 ms of each create in `registry_commit`, even with no fsync.
  Reads leave the writer turn, and each write runs one statement fewer: the
  in-transaction record read comes from the index, and the file check runs
  before the turn. Each remaining statement still releases the GIL under the
  turn. The next lever is one writer with group commit, as
  `DurableSqliteBatch` already does for the memory and storage journals.
- **On ext4 the bottleneck moves to the network lease write.** Its file and
  directory fsync run under the global state lock. With registry commits
  2–3x faster, 32 creates reach that lock together. That write is slot
  authority and stays synced; group commit would apply there too.
- **NORMAL for `commit_owned` is neutral on tmpfs and within noise on ext4.**
  It adds two `PRAGMA synchronous` statements and removes one WAL fsync from
  every create. Its value scales with real fsync latency on the node's disk.
- **Heartbeat at 500 registrations is about 18x cheaper.** 92% of the old
  build recomputed spec fingerprints: three SHA-256s of `asdict` JSON per
  registration per heartbeat. The registry's SQLite reads were about 1.3 ms
  of the 98–107 ms.

## Reproduce

```sh
# For each code tree (TREE), medium and run:
unshare --user --map-root-user --net --mount --fork \
  .venv/bin/python docs/benchmarks/create-pipeline-2026-10-02/bench_create_pipeline.py \
  --code "$TREE" --state "$STATE_DIR" --out result.json [--tmpfs]
```
