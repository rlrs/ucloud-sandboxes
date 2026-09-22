# Native write-volume qualification on an idle production worker

User authorized production use after the Linux 5.15 development host blocked
native qualification. Ran on 2026-09-22, approximately 09:14–09:19 UTC, on UCloud
worker **12398499**, kernel **7.0.0-30-generic**, 32 vCPUs and roughly 88 GiB RAM.
Project: `4827bd3a-4e74-4393-9b82-49f71636c141`.

Preflight found no routed sandboxes and no active operations on the six workers.
Eight old `wake_storage_recovery_required` pending records were present in the
gateway; this test did not alter them or establish their cause.

Tests used the exact serving backend artifact:
`75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`.
Each test started its own daemon, Unix sockets, caches and disposable layers or
XFS devices. The package under `/var/tmp/wvq-20260922` is isolated from serving
Python packages. `/var/tmp` is on local `/dev/vda1`; `/tmp` is tmpfs and was avoided
for the data files. No service restart, serving code deployment, automatic trim,
provider lifecycle change or test sandbox reservation was performed.

## Compaction output

`runtime/storage_native/benchmark_tiered_compaction.py` compares the previous
policy with size-tiered suffix selection. A 64 MiB base receives repeated 2 MiB
hot overwrites, zero writes and discard markers, with an eight-layer depth target
and 4 MiB delta trigger. Logical contents are reconstructed and verified after
**every** append/merge. This is a synthetic checkpoint trace, not an agent run.

| Test | Previous exported bytes | Tiered exported bytes | Reduction |
| --- | ---: | ---: | ---: |
| 32 cycles, one trace | 1,073,938,432 | 134,479,872 | 87.48% |
| Four concurrent traces, 64 cycles each | 8,591,507,456 | 1,075,838,976 | 87.48% |

All data checks passed. The concurrent test completed in 45.08 seconds. Both
policies ran 32 merges per 64-cycle trace; savings came from excluding older data,
not skipping checkpoint verification or reducing the number of merges. The final
chains had one layer for the previous policy and two for the new policy.

These are actual native export byte counts, not whole-machine physical writes.
Timings include Python verification; previous policy runs first and cache/order
are not controlled, so timing differences are not a production speedup claim.
The concurrent test is **256 checkpoint cycles, not 256 simultaneous sandboxes**.
I/O PSI some avg10 rose from 0.31% to 1.21%; it did not reproduce the earlier
production storage saturation. Warm-retention savings and wake p95 remain unmeasured.

Artifacts: [single trace](tiered.json), [concurrent traces](concurrent.json).

## XFS trim

`runtime/storage_native/qualify_xfs_trim.py` created private 1 GiB XFS volumes,
wrote a 64 MiB subsequently deleted file, retained data, zeros and a sparse hole.
Baseline and trim branches resumed the same base. Trim used sixteen 64 MiB
FITRIM windows, with a 1 MiB minimum extent. Full exports and partial delta exports
were both restored through native devices and remounted; retained data hashes and
deleted-file absence passed.

Without concurrent foreground work, the full export shrank from 273,473,536 to
206,233,600 bytes: **67,239,936 bytes (64.125 MiB)** removed. Each trim window took
3.6–9.1 ms. Suffix export size stayed 2,158,592 bytes: trim primarily eliminates
obsolete data when older tiers are merged; it does not immediately shrink a
sealed base file or necessarily reduce each new delta.

A second run kept a foreground thread performing 4 KiB fsync writes plus 8 MiB
reads during trim. Data verification and the same 64.125 MiB export reduction
passed again. Sixteen trim windows had a 9.14 ms median and 18.18 ms maximum.
Foreground operation times were:

| Case | Samples | Median | p95 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 34 | 3.07 ms | 7.61 ms | 23.56 ms |
| Concurrent trim | 43 | 2.69 ms | 4.75 ms | 19.00 ms |

This short test found no foreground correctness or latency problem; these small
samples do not establish a latency improvement or a production SLO. Runtime
trimming remains disabled. A pressure-aware policy must coordinate with volume
ownership and freeze/seal, pace incremental work, and avoid repeatedly scanning
large volumes. Results qualify this kernel/backend combination, not every backend.

Artifacts: [trim](trim.json), [concurrent trim](trim-concurrent.json).

## Cleanup and health

Both test volume trees were removed after confirming no test mount remained.
Test daemons shut down and their devices were deleted by the harness. The serving
backend retained PID **5496**, all 16 warm pool devices, zero active devices,
zero active/waiting storage operations, zero error volumes, and a successful
node-agent health response. [Post-test check](worker-after.log).
