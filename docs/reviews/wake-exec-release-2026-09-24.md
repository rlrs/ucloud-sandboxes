# rc24 deployment and measured remaining latency

Deployed 2026-09-24 08:03:35 UTC, server `5bc348f` / 0.5.114rc24. Both node role
bundles preserve rc23 native/OS/storage files byte-for-byte. 142 installed package
files match the release wheel. PostgreSQL authority and sandbox policy were
preserved. Gateway, relay and autoscaler are active. The initially idle fleet
had no existing workers requiring replacement; live test worker 12401155 ran rc24.

At 08:04:33 UTC the user-authorized worker cap increased from 8 to 10, leaving
min_nodes=0 and target_memory_utilization=0.8. The deployment receipt records the
original cap; worker-cap-change.json records the later effective change.

SDK 0.4.27 (`dc62af77cdb194d2c567e33e4b5b615ffa7c22d0`) is published on GitHub.
SDK main is fast-forwarded to that release. CI run 35972821996 passed the full
Inspect-enabled suite, lint and wheel checks on Python 3.10/3.13. Verifiers main
`9226b1f` pins the immutable SDK wheel; 16 plugin tests, Ruff and ty passed.
Existing external caller environments still need to pull/sync this update.

Targeted Linux checks: 123 server tests and 58 SDK client/duplex tests passed.
The sparse-file fixture passed on Linux. The initial-snapshot timing test was
corrected to await bounded child completion rather than assuming the 50 ms
snapshot wait guarantees completion. Broader tests in the gateway qualification
environment encountered unrelated 512-connection timing failures and missing
Inspect dependencies; successful clean SDK CI provides that broader gate.

## Live verification

All 16 concurrent managed-profile creates and 48 forced park/wake cycles passed;
no scenario, cleanup, or wake error events. The test forced synchronized parking
on a single 32-vCPU worker with 2 GiB guest limits and 128 MiB resident heaps.
Measured post-warmup useful execution p95: 2.519s; guest continuation p95: 2.201s;
response commit p95: 0.030s. SLO failed both latency and insufficient fleet-health
observation (test ended within the new worker's 30-second grace; no health failures).

Separate managed-profile exec checks preserved 3,300,000 UTF-8 bytes on each of
stdout/stderr in both sync (0.848s) and async (0.785s) full-duplex modes. An 8 MiB
stdout command with a deliberately delayed reader completed without sequence
loss. All test routes were deleted; remaining route count verified zero.

An initial plain-container-profile canary did not establish a live runtime:
its journal entered recovery-required, and create kept retrying. It was cancelled
and deleted. This is not included in successful results or claimed fixed; plain
container startup/recovery retry handling needs separate investigation.

## Remaining bottleneck

Seven sampled wake traces locate most worker time in restore preparation and
validation (mean 308 ms, max 622 ms) and synchronous artifact cleanup (mean 152 ms,
max 290 ms). Together these account for about 56% of sampled mean warden time
(827 ms). The validation bucket also includes mount/rebind, component checks,
backing preparation and source retention; it is not proof of hashing cost alone.
Native runsc restore averaged 205 ms (144–294 ms). Restore queue wait averaged
49 ms (max 128 ms). Lifecycle lock waits were below 0.2 ms; growth admission was
5–115 ms. Gateway admission was 6–9 ms, and health probes roughly 9–20 ms.

A representative 1.178s warden restore spent 622 ms in preparation/validation,
247 ms in cleanup, and 193 ms in native restore. Python thread CPU for its wake
span was 62 ms. Available RAM was roughly 82 GiB, CPU about 25% in the loaded
sample, and I/O PSI some about 13.4%. These are sampled snapshots, not a complete
I/O throughput profile or proof that Python cannot be a bottleneck elsewhere.

Next investigation: split the broad preparation timer and remove safely deferrable
cleanup from the critical path with durable ownership/retirement fencing. Do not
weaken identity/integrity checks or silently restore the unqualified deferred
reclamation branch. More workers may distribute a larger workload but do not
remove this per-wake storage work.

Evidence: ../benchmarks/wake-exec-release-2026-09-24/.
