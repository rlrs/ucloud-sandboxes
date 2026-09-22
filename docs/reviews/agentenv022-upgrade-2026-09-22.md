# AgentEnv v0.2.2 storage integration and migration qualification

The repository now builds AgentEnv `771ea55ca80abbfacc85e716ec91c40e82b3398b`
(v0.2.2) with the local storage contract preserved. Production has **not** been
switched to this backend. Rollout requires fresh writable state; replacing the
daemon on an occupied worker is not supported.

## Integration

The six existing patches still provide streamed dense/compacted exports,
exclusive pooled deletion, owner identity, atomic owner transitions, premerged
index identity and delayed idle-device retirement. Constructor changes were
ported to the new synchronous `LocalFile` API. Upstream cache recovery and
allocation/eviction fixes were retained. The upstream pool failure test now
checks the local single-flight exponential backoff rather than an obsolete
helper.

Inspection of the actual tag found that the daemon did **not** activate
jemalloc, despite the earlier upstream allocator change. The seventh patch
reapplies upstream commit `e4ae61d6dff3a45f56a9ba6de7a262e72ddb75ba`.
The eighth patch stamps fresh hybrid data/index headers with sub-version 2
and rejects unmarked or unknown hybrid uppers before replay. Sealed read-only
layers retain their compatible format. Both packaging and VM boot validate
all eight patches and the writable-format marker; packaging previously
checked only three of the six required patches.
VM initialization additionally refuses to replace an already-installed old or
unmarked backend before package installation or service restarts. Fresh base
images and images baked with the new backend are supported; old worker image
templates need rebuilding, not bypassing the guard.

Relevant upstream changes: [v0.2.2 release](https://github.com/kvcache-ai/AgentENV/releases/tag/v0.2.2),
[hybrid discard/rewrite and compatibility](https://github.com/kvcache-ai/AgentENV/pull/255),
[premerged-index pruning fix](https://github.com/kvcache-ai/AgentENV/pull/244),
and [cache allocation safety](https://github.com/kvcache-ai/AgentENV/pull/217).

## Build and correctness evidence

The canonical `build_pinned.sh` ran on Linux with Rust 1.98.1, starting from a
clean exact-tag checkout. All eight patches applied and were reversed after
the build, leaving the checkout clean. It reproduced the same binary used
in the VM tests:

| Artifact | SHA-256 |
| --- | --- |
| Production v0.1.2 baseline | `75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7` |
| Qualified v0.2.2 candidate | `76d59a1c10cb495e90380e320de8fe30e6370f8b374aab410494c8e0b92a5748` |

The [build manifest](../benchmarks/agentenv022-2026-09-22/build-manifest.json)
records every patch digest. The targeted Rust suite passed **217 tests**:
103 LSMT file/index tests, 54 cache tests and 60 daemon tests; three upstream
tests remained ignored. Coverage includes failed/interrupted eviction,
foreground refill failure, discard/rewrite reuse across reopen, owner release
races, device reuse/backoff, premerged identity and old-upper rejection.
The Linux Python checks also passed: 79 storage-native tests and 29 deployment/
VM initialization tests. Tests ran with `umask 077` because the hibernation
fixtures require private directories; the VM's default group-writable umask
caused two fixture failures before correction. Deployment fixtures included
the referenced Hetzner installation script. Ruff and shell syntax checks passed.

Cross-version tests ran on disposable UCloud VM **12399394**, with 8 vCPUs,
24 GB RAM, a 100 GiB disk, Ubuntu 26.04 and kernel `7.0.0-30-generic`.
It had no production worker identity or sandbox assignments.
After collecting results, all test devices/processes and namespace/cache
mounts were released and the qualification VM was stopped. The final cleanup
verification is recorded in [cleanup.json](../benchmarks/agentenv022-2026-09-22/cleanup.json).

The [migration results](../benchmarks/agentenv022-2026-09-22/migration.json)
passed all three cases:

- v0.1.2 seal/export → HTTP range reads → v0.2.2 fresh upper.
- v0.2.2 seal/export → HTTP range reads → v0.1.2 fresh upper (rollback).
- v0.1.2 → v0.2.2 with only **64 KiB free** on a bounded 64 MiB destination
  cache filesystem. Restore and data verification passed; the daemon survived.

Each case verifies ext4 metadata, overlayfs behavior, a 256 MiB mmap memory
image with hashes/sentinels, dense-stream digest/length and filesystem ENOSPC.
Upgrade cases also attempt to reopen a legacy upper and require rejection
without modifying either data or index. The full-cache case served about
31 MB of remote reads in the filesystem-only run.

The [gVisor migration run](../benchmarks/agentenv022-2026-09-22/migration-gvisor.json)
then passed all three cases using the actual production gVisor executables
and statically built `conformance_workload.c`/`noop.c`. It checkpoints and
destroys the sentry on the source, reconstructs storage on the destination,
restores the process, executes a new command and verifies retained process
state. The conformance workload exercises threads, pipes, sockets, timers,
signals, mappings and an unlinked open file. All three resumed with
`ok 8590456832`. Restore command times were 218 ms for upgrade, 268 ms for
rollback and 269 ms with the nearly full cache. These isolated timings exclude
production coordination and are not a loaded wake SLO. The full-cache run
served about 90.5 MB of HTTP reads without losing the daemon.
The copied production runsc reported version `50e1502a95d3-dirty` and SHA-256
`be491ee25a10a9036b46037dbd21343cc3bf4a4eee90ca21885e7848124bd9e5`;
all four companion executables were copied with it.

The [local compaction test](../benchmarks/agentenv022-2026-09-22/local-compaction.json)
preserved overwritten data, explicit zeros, discard mappings and holes across
29 initial layers plus 24 additional checkpoint cycles. Five compactions were
adopted with no failures; the final chain had three layers. It also verified
wake and newly appended layers during compaction.

An initial full-cache attempt exposed a qualifier bug: its HTTP server shared
the Python process with mmap verification, and a page fault could hold the GIL
while waiting for that server. The qualifier now serves blobs from a separate
process. Results above are from the corrected rerun; the failed harness run
is not counted as evidence of backend safety or failure.

## Pooled lifecycle comparison

Both binaries ran sequentially on the same otherwise idle VM, using the real
Python storage service, journal, XFS mounts and a 2/16 warm-device pool. Each
run had 100 sequential wake/release cycles followed by 20 rounds across eight
volumes (260 cycles total). Every cycle passed; both ended with zero hard
reservations. Memory was sampled once per second and after 15 seconds idle.

| Measurement | Patched v0.1.2 | Patched v0.2.2 |
| --- | ---: | ---: |
| Sequential mount p95 | 39.2 ms | 37.3 ms |
| Eight-way mount p95 | 112.1 ms | 99.9 ms |
| Eight-way release p95 | 61.0 ms | 51.1 ms |
| Sampled peak anonymous memory | 48.2 MiB | 55.3 MiB |
| Anonymous memory after cleanup and idle | 48.2 MiB | 48.1 MiB |

Raw results: [baseline](../benchmarks/agentenv022-2026-09-22/churn-old-memory.json)
and [candidate](../benchmarks/agentenv022-2026-09-22/churn-new-memory.json).
The latency direction is encouraging, but this is one small sequential A/B
comparison. It does not establish a large memory improvement or a production
wake SLO. Empty parallel volumes and a small index do not reproduce upstream's
600-sandbox memory-heavy workload. Production relay, scheduler, gVisor restore
and external Registry latency are excluded.

A larger comparison used **64 parallel volumes for 40 rounds**, plus 100
sequential cycles: 2,660 cycles per backend. All passed with zero remaining
reservations. Mount p95 improved from **945 ms to 830 ms** and release p95 from
903 ms to 815 ms. Sampled anonymous-memory peaks were 301 MiB versus 317 MiB;
after deletion and 15 seconds idle they were 293 MiB versus 285 MiB. This
supports a modest latency improvement, not a claim of dramatically reduced
memory use. See [64-way baseline](../benchmarks/agentenv022-2026-09-22/churn-old-64.json)
and [64-way candidate](../benchmarks/agentenv022-2026-09-22/churn-new-64.json).

## Sustained I/O comparison

The existing XFS benchmark ran three rounds per binary, alternating target
order against native loopback XFS on the same disk. It used direct I/O, 1 MiB
sequential writes, 4 KiB 70/30 random mixed I/O (eight seconds per sample), and
4,000-file create/stat/rename/delete operations. Hybrid uppers and one I/O ring
match the production defaults.

| Backend median | Patched v0.1.2 | Patched v0.2.2 |
| --- | ---: | ---: |
| Sequential write | 631.6 MB/s | 625.4 MB/s |
| Random mixed | 83,152 IOPS | 88,326 IOPS |
| Metadata workload | 0.654 s | 0.631 s |
| Write bandwidth / native loopback | 56.1% | 56.4% |

Raw results: [baseline](../benchmarks/agentenv022-2026-09-22/io-old.json) and
[candidate](../benchmarks/agentenv022-2026-09-22/io-new.json). Both pass the
metadata, random-I/O and sparse-seal gates; **both fail the existing requirement
to stay within 15% of native sequential write throughput**. The upgrade does
not resolve that existing bottleneck. This A/B does not show a substantial
write regression (about 1%), and mixed IOPS increased about 6%, but the overall
I/O qualifier correctly retains `status: failed` for both builds. Do not report
all performance gates passed.

## Rollout and rollback

Follow the [storage migration procedure](../../runtime/storage_native/README.md#migrating-from-the-v012-backend).
Quiesce and durably publish with the current backend, restore into fresh
workers/uppers, verify actual application execution and another park/resume,
then drain old workers after all state is accounted for. Retain both artifacts.
Rollback uses newly sealed layers and fresh old-format uppers; the old daemon
does not have a guard against opening new-format uppers.

Do not infer migration completion from a parked state or an old heartbeat.
Unpublished state and unreachable owners need explicit accounting. The same
format restriction applies to Hetzner. No SDK or Verifiers update is required
for this native backend change.
