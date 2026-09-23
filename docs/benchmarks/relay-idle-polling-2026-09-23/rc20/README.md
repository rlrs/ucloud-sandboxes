# rc20 production qualification: relay and continuation evidence

The PostgreSQL idle-poll change reduced control-plane write overhead, but the
pressure workload **failed correctness**. This is not a release acceptance or a
sub-second wake result. No production configuration or application source was
changed during measurement.

## Forced park: 16 sandboxes

Run `relay-load-6e95d7896a45`, 2026-09-23 17:26:33–17:28:42 UTC,
completed 64/64 cycles correctly. The 48 measured cycles (excluding warmup)
had continuation p95 **1.055 s** and useful-exec p95 **1.532 s**, compared with
rc19's 1.221 s and 1.719 s. Configuration was unchanged. The repository's full
Linux test gate overlapped the first approximately 75 seconds on the separate
30-core driver; parent observed load average 0.29. This is minor shared-driver
context, not a controlled component A/B.

Five retained wake traces show worker wake 520–655 ms, native restore
242–355 ms, validation 83–162 ms, and foreground artifact cleanup 30–58 ms.
The rc19 retained sample had cleanup 58–309 ms. These are sampled traces,
not fleet percentiles. The gateway-response-to-observer tail was 169–248 ms;
the guest first tool took 64–92 ms. These stages can overlap, so their
subtraction is not an exact transport measurement. The observer timestamp
includes a second guest-originated relay tunnel and polling. Integrity scanning
starts after that receipt. Same-batch observer ACKs are timestamped afterward,
though ACK completion can delay its next poll.

## Pressure: 256 sandboxes

Run `relay-load-8885f7a20e12`, 17:29:16–17:35:02 UTC, completed 1,503 cycles
before stopping after sandbox `0155`, cycle 3, exceeded the existing 180-second
continuation timeout. Default budgets were preserved. Useful-exec p95 was
31.875 s; this is an unsuccessful performance and correctness qualification.

The single 1 Hz read-only PostgreSQL collector sampled 347 times during the
run. No sampled transaction exceeded 0.5 s (maximum 226 ms); pool snapshots
had no queued requests. Aggregate pool wait increased by 2.488 s across the
whole concurrent run. Eight active backend samples waited on WalSync and one
on WALWrite. This cannot exclude short events between samples, but there was
no sustained pool or WAL stall comparable to rc19's initial 3.68-second stall.
Gateway average CPU busy was 34.7%, iowait 0.59%, and physical disk writes
2.06 MiB/s.

WAL synchronization averaged 61.7/s over the run; the two active middle-minute
windows measured 59.4 and 71.8/s, versus rc19's observed active minute near
125/s. Workload stages and completion counts differ, so this is production
corroboration of the isolated ABBA evidence, not a matched causal estimate.
`relay_claim_inference` recorded 21,213 transaction iterations, 18.10 s total
commit-phase time and 1.504 s total pool wait. An empty read-only COMMIT still
increments the transaction metric: this count is **not** a durable-WAL-write
count. rc19 recorded 14,660 iterations, 98.91 s commit and 274.38 s pool wait;
its initial storage stall substantially confounds that comparison.

## Failed continuation: durable evidence

Cycle 3 request `de158ea1287f4e57b31d1aaf110bafe3` stored a status-200 model
response at 17:31:46.313757 UTC. Its wake obligation retried once and completed
at 17:32:48.596351, when delivery was durably released. The first retry time,
17:32:46.435543, matches the separately captured worker's 60-second lifecycle
lock timeout. The final row has `done=true`, two attempts, no last error,
`delivery_pending=false`, and an unchanged transport epoch. The observer
timeout happened at 17:34:46.325586, approximately 118 seconds after release.

Thus PostgreSQL did not retain an unresolved wake/delivery obligation for this
failure. Durable release proves permission to send the response; it does not
prove that the guest received it or executed its next tool. No exact relay
request trace was retained, and completed input bodies are intentionally
removed, so these records alone cannot reconstruct the receiving socket or
identify the later observation receipt. Worker lifecycle/transport evidence
must establish the remaining failure cause.

The collector was stopped after preserving the complete run. Artifacts contain
bounded metrics and lifecycle timestamps, not credentials, registration
secrets, HTTP request bodies, or SQL parameters.
