# What the recorded wake timings establish

The retained concurrency-16 and concurrency-8 runs do **not** measure explicit-wake restore-slot waiting. They support keeping the restore limit at eight until direct queue timing is available. Raising it to sixteen cannot yet be attributed to a measured bottleneck.

Sources: [C16 benchmark](sandbox-density-128-acl-c16-hot-2026-09-07.json), [C8 benchmark](sandbox-density-128-acl-c8-deep-2026-09-07.json), [C16 host analysis](sandbox-density-128-acl-c16-analysis-2026-09-07.json), [C8 host analysis](sandbox-density-128-acl-c8-analysis-2026-09-07.json). Both runs use 128 sandboxes, 256 MiB randomly initialized resident memory, a rotating 16 MiB dirty subset, 100 ms requested CPU work, and a one-CPU sandbox quota. Both predate deployment of the bounded CPU admission wait and both failed some wake admissions.

## Timing boundaries

The [benchmark wake function](../../scripts/benchmark_sandbox_density.py) makes an explicit `POST /wake`, discards that response, and then executes an action. Its service duration covers both operations. The returned `node_exec_start_timings` describes only the later action's exec-start request. Every retained wake sample has `start_ms` plus manager `inspect`, `total` and `exec_lease`; none has `restore_queue` or `restore_*` fields. Small manager timings therefore do not show that explicit restore was fast or that its queue was empty.

The [service](../../ucloud_sandboxes/direct_service.py) acquires a restore slot before network setup and Warden resume, and releases it when that work finishes. The later useful action does not hold a restore slot. The [Warden](../../ucloud_sandboxes/direct_warden.py) already fills a restore timing dictionary, but these benchmark results did not retain it.

## Recorded successful-operation timings

| Measurement | C16 act | C16 wake then act | C8 act | C8 wake then act |
| --- | ---: | ---: | ---: | ---: |
| Successful samples | 128 | 121 | 128 | 117 |
| Service p95 | 775 ms | 2,766 ms | 631 ms | 1,732 ms |
| Guest total p95 | 510 ms | 955 ms | 514 ms | 722 ms |
| Full-memory verification p95 | 211 ms | 564 ms | 208 ms | 433 ms |
| Dirty-subset update and full hash p95 | 141 ms | 150 ms | 141 ms | 190 ms |
| SQLite commit p95 | 0.77 ms | 0.92 ms | 0.63 ms | 0.80 ms |
| Subsequent exec-start p95 | 227 ms | 252 ms | 89 ms | 96 ms |
| Subsequent exec manager total p95 | 9.15 ms | 10.82 ms | 4.15 ms | 4.38 ms |

These are separate nearest-rank percentiles, not additive components of one p95 operation. Failures are excluded from these duration percentiles and remain failures: seven for C16, eleven for C8.

Subtracting each operation's guest duration from its own service duration gives a mean host/client remainder of **1,202 ms for C16 wake** and **664 ms for C8 wake**, compared with 162 ms and 94 ms during act. This remainder includes explicit wake, exec startup and completion, and request overhead; it is not a measured restore duration. Most of the increase in measured guest time comes from full-memory verification: mean 334 ms after C16 wake versus 173 ms during act, and 253 ms after C8 wake versus 171 ms during act. SQLite commits are not the observed guest bottleneck.

The C16 journal also rules out a large normal post-output exec-exit tail in this run: wake probes had process-wait p95 1,216 ms, final-stdout-to-wait-end p95 22.4 ms, and maximum tail 59.0 ms. Completion callbacks had p95 42.0 ms. They cannot explain the full explicit-wake remainder.

## Restore concurrency conclusion

C8 submitted at most eight wake-through-action requests concurrently on the dedicated idle test node. With no competing restore workload, an eight-slot restore semaphore cannot constrain that run: at most eight callers can need a slot, and each releases it before its action. Its 664 ms mean non-guest wake remainder therefore exists without a client burst large enough to queue behind this limit.

C16 can queue behind eight slots, but the evidence does not separate that queue from storage mount, artifact validation, runsc restore/readiness, or host contention. C16's higher throughput comes with larger per-operation latency; reducing client concurrency improved service p95 but lengthened queued completion and the whole wake wave. Doubling restore slots is not equivalent to doubling available throughput.

The host samples confirm retained one-CPU leaf quotas for all 128 residents. This avoids the earlier unlimited startup-quota bug, but does not prove that sixteen simultaneous restores improve end-to-end latency: host-side runtime processes, storage work and the eight-operation storage service also share capacity.

The next diagnostic should record explicit wake total, admission-enter time, exact restore-slot acquire time, network setup, and the Warden's existing phase dictionary. Only a material measured restore-slot queue, together with spare host capacity and acceptable storage behavior, would support a controlled comparison of eight versus sixteen slots. No restore configuration was changed for this analysis.
