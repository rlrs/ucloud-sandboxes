# Workspace formatting I/O qualification

The rc9 natural256 run completed 2,048 turns without parking, publication or
compaction. Whole-worker guest-visible leaf-device counters reported 87.87 GB
written, versus 8.59 GB of guest file payload. These counters include filesystem
metadata, writeback and provisioning; they do **not** establish an 10.2× native
backend amplification ratio. Most writes coincided with provisioning/initial
dirty-cache flush. The last sample preceded cleanup completion.
`production-attribution.json` retains the relevant source counters and timestamps.

A concrete avoidable component is filesystem initialization. Production uses
xfsprogs 6.18.0, whose SSD default sizes the journal from host CPU count. That gives
each one-vCPU sandbox a journal intended for the whole 32-core worker. The
upstream `mkfs/xfs_mkfs.c` functions `calc_concurrency_logblocks` and
`calculate_log_size` show the mechanism:
https://kernel.googlesource.com/pub/scm/fs/xfs/xfsprogs-dev/+/refs/tags/v6.18.0/mkfs/xfs_mkfs.c

The implementation selects `-l concurrency=0` only when mkfs's **log** help section
advertises support. This disables host-CPU inflation while retaining filesystem
size-based sizing and minimum journal requirements. It is not a fixed-size cap:
a 1 TiB dry-run still chooses a 512 MiB journal. Older xfsprogs keep their existing
behavior. Reflinks, CRCs, journal durability and discard remain enabled.

## Native results

Disposable UCloud VM 12400373, Ubuntu 26.04, Linux 7.0.0-30-generic,
xfsprogs 6.18.0, 4 vCPU/12 GiB. Each run owns a new 4 GiB loop device. Explicit
`concurrency=32` reproduces the production journal geometry. Every 15-second
sample mixes 64×64 KiB repository-file rewrites with SQLite WAL `synchronous=FULL`
transactions, then verifies database integrity and payload hashes after remount,
and passes `xfs_repair -n`. Ordering is ABC-CBA, two samples per mode. No global
cache drop or production mutation was used.

| 4 KiB filesystem sector, configuration | Format writes | Transactions/s | Transaction p95 |
| --- | ---: | ---: | ---: |
| Host 32-core journal, unrestricted | 223.09 MB | 763 | 4.25 ms |
| Filesystem-sized journal, unrestricted | 67.69 MB | 704 | 4.69 ms |
| Host journal, 1-CPU quota, evidenced repeat | 223.09 MB | 773 | 4.35 ms |
| Filesystem-sized journal, 1-CPU quota, evidenced repeat | 67.69 MB | 759 | 4.33 ms |

Formatting writes fell **69.7%**. The unrestricted run had a **7.8% transaction
throughput regression**; two quota-constrained repeats ranged from 0.8% faster to
1.9% slower with the smaller journal. The evidenced repeat records
`cpu.max=100000 100000`, actual use around 0.30 core and no throttling. The test is
fsync-bound: CPU quota cannot be claimed as the cause of the difference.
The 512-byte-sector comparison was approximately flat (747 vs742 txn/s).

These are small loop-backed host-filesystem tests, not a production backend
throughput qualification. The 4 KiB geometry matches production XFS sectors but
does not reproduce the ublk logical-512/physical-4096 queue or fleet contention.
Expected savings across 256 identical creates are about 38 GiB of device writes;
that is an estimate from isolated format counters, not a measured production
saving. Latency/steady-state regression remains a rollout check.

`summary.json` contains means. Raw outputs retain exact geometry, syscall/device
counters, correctness checks and, in `sector4096-onecpu.jsonl`, cgroup CPU evidence.
Reproduce with `runtime/storage_native/qualify_workspace_format.py` under a private
systemd scope (`CPUQuota=100%`, `CPUQuotaPeriodSec=100ms` for quota comparisons).

## Follow-up: immutable blank filesystem seed

The next startup optimization can reuse existing native lower layers and
per-volume hardlink pins. Seal a freshly formatted **never-mounted** filesystem,
record its format identity in the existing volume journal, and pin it into another
already-reserved volume before acquiring that volume's writable upper. Such a
seed needs no independent persistent cache or GC authority: it survives only
while normal journal-owned volumes retain links. If the final link disappears,
the next create formats again.

Required proof before enabling: immutable blank provenance, size/sector/formatter
compatibility, concurrent first-create serialization, restart and interrupted-pin
recovery, source deletion/compaction racing target pin, independent copy-up and
normal publication/import. Keeping a permanent cache link outside those ownership
records would retain physical blocks after their hard reservation was released;
that approach is explicitly rejected. This follow-up is deferred while the
remaining steady-state gateway latency is the larger bottleneck.
