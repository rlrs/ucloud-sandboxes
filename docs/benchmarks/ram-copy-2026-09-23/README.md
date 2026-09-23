# RAM restore copy alternatives, 2026-09-23

**Keep the existing 256 KiB buffered sparse-copy implementation.** Neither a
larger buffer nor `sendfile` showed a repeatable improvement in this bounded
qualification. No native runtime patch, fingerprint or production setting changed.

Disposable VM12400373 ran Ubuntu26.04/Linux7.0.0-30-generic,4vCPU/12GiB.
The benchmark compiled with Go1.27.1 on Linux. `main.go` contains the exact
`CopySparseApplicationMemory` body from runtime patch0006 (standard-library
`syscall` substituted for `x/sys/unix`), plus comparison code. This is an extracted
function benchmark, not a rebuilt/pinned gVisor end-to-end restore measurement.

Each source is a 1GiB logical XFS file with512MiB random allocated data and a
512MiB punched hole. The target is private `tmpfs,noswap`. Each copy runs in a
fresh systemd cgroup with `cpu.max=100000 100000` and `memory.max=2GiB`. Results
record actual CPU use, throttle time, faults, allocation and cgroup shmem increase.
Hashing/validation occurs after the measured copy. Every successful sample
preserved source/target hashes and sparse allocation; target pages increased the
copying cgroup's shmem by exactly536,870,912bytes.

`copy_file_range` returned EXDEV for XFS→tmpfs. The `sendfile` alternative uses
SEEK_DATA/SEEK_HOLE and an explicit source offset to preserve sparse regions.

| Repeated comparison, seconds | Current256KiB | Buffer1MiB | sendfile |
| --- | --- | --- | --- |
| Warm-source samples | .390 / .205 | .462 / .263 | .298 / .218 |
| Per-file eviction requested | .255 / .258 | .257 / .257 | .253 / .261 |

Eviction uses source-only `POSIX_FADV_DONTNEED`; it does not prove the entire
underlying storage path is physically cold. No global cache drop was used.
In the first comparison, sendfile took2.64/2.70s after requested eviction while
the existing implementation took.26/.66s. That difference did not reproduce.
The raw first and repeat results are retained rather than selecting only the
favorable numbers. About.25s/.25CPU-seconds per512MiB is a useful local baseline,
not a universal throughput bound or a decomposition of production runsc_restore.

An initial fixture was discarded: sequential XFS writes followed by extending EOF
left speculative unwritten extents; reading the full file for hashing made those
extents SEEK_DATA-visible. The controlled fixture explicitly punches its intended
hole and checks allocation, avoiding comparing512MiB with1GiB of copied pages.
This does not establish a production bug: production export pre-truncates before
sparse writes and does not perform the same whole-file validation reads.

Reproduction source is retained as `main.go` and `run.py`. Build `main.go` for Linux
as `/home/ucloud/ram-copy-benchmark`, then run `run.py` as root only on a disposable
host. It creates an exclusively owned root and loop image, verifies loop ownership
before detach, and unmounts/removes only its own fixtures. No production systems
were altered for these tests.
