# Memory backing qualification — 23 September 2026

The private file-clone path removes eager application-heap copying from restore.
File-backed application pages can then be reclaimed while keeping the same
runtime, TCP connections and mounted workspace alive. This is a native mechanism
and ownership qualification, **not a production density or p95 claim**.

## Environment and retained artifacts

Owned UCloud job 12400688: Ubuntu 26.04, Linux 7.0.0-30,4 vCPU / 12 GiB, local 32 GiB
XFS image mounted with project quotas, discard and verified direct loop I/O.
Each guest had one CPU and a 2 GiB memory cgroup. The C workload holds a 1.5 GiB
incompressible heap, checks every 64-bit word, changes 384 MiB each turn, retains a
TCP connection and commits SQLite WAL transactions with synchronous FULL.
The job was stopped after artifact transfer; UCloud reported SUCCESS at
16:54:08 UTC. No production worker was used.

`native-build-manifest.json` attests the complete seven-patch runtime:

- runsc SHA256 `203d43a11e05fc836863aa40afee89b70b93403fcb73647c60c4df29e84b2f35`;
- upstream commit `50e1502a95d36ad2faf2c7ef33b8bf21fe975293`;
- seventh patch SHA256 `17933cd7990ac28c0b9bdb8add8330f242fdd3dcf61621c2a7d3905c984f6619`.

`pinned-build-gates.log` contains the successful canonical build/test pipeline.
The default host filesystem does not support reflink, so its Go helper test can
skip. `reflink-xfs-go-test.log` separately proves that test **passed without a
skip on XFS**. Other source tests and the full release build passed. On this
Ubuntu image, root tests used a small run-under wrapper that explicitly passed
Bazel TEST/RUNFILES variables through sudo and returned generated outputs to the
build user; sudo-rs ignores `-E`.

## Complete product path

`product-hybrid-reclaim.json` uses the actual Warden, direct registry capacity
ledger, memory allocator, storage Unix protocol, ublk workspace and overlay
manager. It starts in RAM and injects a precommit capture failure; abort returns
to the exact original runtime. It then completes three capture/restores, each
with a 384 MiB mutation before capture and complete heap/TCP/SQLite validation.

Each restore converts to the persisted file mode. Total Warden wake was
350–382 ms. After restore, canonical `flush_reclaimable_memory` took 7.4–7.9 ms;
cgroup reclaim freed 1,545.6–1,545.9 MiB in 86–92 ms. The journal, PID and incarnation
remained unchanged. Full integrity checks after those page faults took 526–553 ms.
Final deletion left zero memory allocations, zero memory hard claims and zero
reflink overlap claims. These timings are from a single guest on an isolated VM.

`product-hybrid-checkpoint.json` is the earlier product lifecycle pass without
the live-reclaim step: three wakes 340–465 ms. Its sampled cgroup shmem after
conversion is below 0.2 MiB; the heap is file-backed and remains charged to the
same 2 GiB sandbox limit while resident.

## Native A/B evidence

`abba-{a..f}-*.json` is an A/B/C/C/B/A sequence of four turns per trial. A uses
RAM full capture/restore, B uses private file-clone capture/restore and C retains
the runtime while explicitly flushing/reclaiming file-backed memory. All 24
turns passed integrity checks. Counters include the dirty phase:

| Operation | RAM full park | File clone park | File live reclaim |
|---|---:|---:|---:|
| Steady physical writes per turn | about 1,537 MiB | about 385–389 MiB | about 385–389 MiB |
| Restore plus state/resume |731–1,601 ms |78–127 ms | no restore |
| Paused candidate charge |about 1,563 MiB |about 25 MiB | same runtime |
| Reclaim operation |full capture required |full capture |177–276 ms |
| Full heap check after reclaim |— |— |349–557 ms |

The RAM trial also had 8.0 s and 10.7 s capture outliers; they remain in the raw
reports. Whole-VM disk counters include filesystem writeback and are not guest
exclusive. One file trial wrote 3.813 GiB versus 3.032 GiB in its paired trial;
we do not attribute that extra volume to guest data alone. Earlier
`file-park.json`, `file-reclaim.json` and `ram-park.json` exclude the dirty phase
from per-turn counters and are preliminary, not the write-volume comparison.

The additional `cold-{a..d}-*.json` ABBA flushed and evicted only the completed,
owned checkpoint file before restore, after its runtime was gone. It never
dropped global caches or live guest pages. All 12 turns passed. Both modes read
about 4.50 GiB from the physical device across three turns. RAM wrote 4.506–4.507 GiB;
file mode wrote 2.635–3.331 GiB, including initial heap construction. Paused restore
was816–1,734 ms for RAM versus115–216 ms for file. Full-heap checking subsequently
took 194–396 ms for RAM and 574–903 ms for file: demand paging defers reads until
application access; it does not eliminate them. A workload touching every page
immediately will pay that cost.

File-backed memory can still cause background writes in low-pressure periods.
The intended hybrid starts in RAM and converts after an actual durable park;
this evidence does not justify claiming zero writeback or an unconditional win
for every workload.

## Immutable source and quota gates

`native-reflink-proof.json` checks the original complete checkpoint hash after a
384MiB guest mutation through the private clone. An injected corrupt kernel
checkpoint makes candidate restore fail after cloning; the original source is
unchanged, failed candidate state is cleaned, and retry succeeds. This tests the
original checkpoint's preservation, not just successful restore contents.

`reflink-quota.json` demonstrates that logical project quota is charged for each
clone even when physical extents are shared. A 40 MiB source cannot clone within
a 64 MiB project (ENOSPC); a 128 MiB project works. Distinct 64 MiB source/target
projects support cross-project reflink and preserve the source after mutation.

`retain-file-proof.json` invokes the **production** `XfsMemoryQuota.retain_file`
FSSETXATTR path: a 40 MiB source inode moves from the live 64 MiB project into an
exact 40 MiB retention project without moving its path. Cloning back into the
unchanged live 64 MiB project and modifying the candidate leaves the source hash
unchanged. The assembled product test also exercises reserve, transfer, clone,
durable handoff and cleanup through their actual owners.

Cgroup memory.max bounds resident memory, not the total allocated size of a
reclaimable file. Observed private/kernel usage is not a universal restore bound.
The production implementation must retain disk quota and physical claims until
source cleanup, and must keep conservative memory admission where evidence is
unknown.

## Reproduction

Build the pinned runtime using `runtime/gvisor/build_pinned.sh`. On an isolated
Linux XFS/project-quota root, compile the static workload:

```sh
gcc -O2 -static -pthread -Wall -Werror \
  runtime/storage_native/memory_tier_workload.c -lsqlite3 -ldl -lm -o workload
```

Run `benchmark_memory_tiers.py` with absolute `--runsc`, `--workload`,
`--work-root` and `--output` paths. Select `--mode ram-park`, `file-park` or
`file-reclaim`; include `--native-reflink`. Use `--cold-restore` for a matched
cold checkpoint comparison and separate `--prove-immutable --prove-failure`
from latency measurements.

Run `qualify_split_checkpoint.py` with its real storage backend and noop fixture,
`--memory-mb 2048 --ram-active --reflink-restore --dirty-command dirty`. This exercises the product path with
real kernel quota and cgroup enforcement. No production deployment is performed
by these fixtures.
